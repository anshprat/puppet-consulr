require 'net/http'
require 'timeout'
require 'base64'
require 'json'
require 'uri'

module Puppet::Parser::Functions
  newfunction(:consulr_kv, :type => :rvalue) do |args|

    # Default config
    default_config = {
      'uri'           => 'http://localhost:8500',
      'config_type'   => 'config_state',
      'scope'         => 'host',
      'scope_param'   => '',
      'value_only'    => true,
      'data'          => 'variable=value',
      'action'        => 'set',
      'facter'        => 'fqdn',
      'base64_decode' => true,
      'ignore_404'    => true,
      'token'         => false,
      'timeout'       => 5,
      'hostname'      => '',
    }

    # Required config options (for future use)
    required_config = []

    # User config
    user_config = args.first ? args.first : Hash.new
    # final config
    config = default_config.merge(user_config)

    # Missing config
    missing_config = required_config.reject {|i| config.has_key?(i)}

    raise Puppet::ParseError, "Consulr missing config: #{missing_config.join(', ')}" unless missing_config.empty?

    if config['hostname'] == ''
      if config['scope'] == 'host' 
      # lookupvar returns 'nil' if the fact doesn't exist...
        hostname = lookupvar(config['facter'])
        # ...so lets raise hell if thats the case.
        raise Puppet::ParseError, "Consulr facter not found: #{config['facter']}" if hostname.nil?
      else
        # Lots of things to be added presentlt, just raise exception presently
        raise Puppet::ParseError,  "Consulr hostname not found" if hostname.nil?
      end
    end
    consulr = Hash.new

    begin
      Timeout::timeout(config['timeout']) do
        # Build and parse URI
        if (config['scope'] != 'global' and config['scope_param'] == '')
          raise Puppet::ParseError, "scope_param should be given when scope is not global"
        end
        if config['action'] = 'get'
          role = /(^.+?)./.match(config['scope_param'])
          order = [['global',''],['role',role],['host',config['scope_param']]]
          order.each do |u|
            kv_uri = "#{config['uri']}/v1/kv/#{config['config_type']}/#{u[0]}/#{u[1]}/"
            kv_uri = kv_uri.chomp('/')
            kv_uri << '?recurse'
            begin
              response = Net::HTTP.get_response(URI.parse(kv_uri))
              rescue Errno::ECONNREFUSED
                return consulr
            end
            # Following HTTP codes will not raise an exception
            ignore_http_codes = ['200']
            # Option to ignore 404
            ignore_http_codes << '404' if config['ignore_404']
            raise Puppet::ParseError, "Consulr HTTP error: #{kv_uri} (#{response.code}: #{response.message})" unless ignore_http_codes.include?(response.code)
            data = JSON.parse(response.body) rescue []

        # Iterate though the keys and put them in a hash 
            data.each do |kv|
              if kv['Value'] != nil
              # We are determining 2 things here:
              # 1) Whether to send back only value or the entire hash or
              # 2) Whether to decode the value before sending it back
                if config['value_only']
                  result = config['base64_decode'] ? Base64.decode64(kv['Value']) : kv['Value']
                else
                  kv['Value'] = Base64.decode64(kv['Value']) if config['base64_decode']
                  result = kv
                end
              end
              if result != nil
                consulr[kv['Key'].split('/')[-1]] = result
              end
            end
          end
        end
      end
    rescue Timeout::Error => e
      raise Puppet::ParseError, "Consulr timed out: #{e.message}"

    rescue => e
      raise Puppet::ParseError, "Consulr exception: #{e.message}"

    end

    return consulr
  end
end
