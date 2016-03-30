#!/usr/bin/env ruby
# Syncs a data center's records from miniiDB to iDB
require 'rubygems'
require 'optparse'
require 'net/https'
require 'json'
require 'logger'
require 'time'
require 'openssl'
require 'uri'

module MiniIDB
  class Sync
    API_VERSION = '/api/1.03'
    LAST_LEVELS = ['unmanagedhost', 'host']
    CHILDREN_MAPPING = { 'datacenter' => ['superpod'],
                         'superpod'   => ['cluster'],
                         'cluster'    => ['host', 'unmanagedhost']}

    def initialize(options)
      required_keys = [:miniidb_host, :idb_endpoint, :datacenter]
      required_keys.each do |k|
        raise ArgumentError.new(sprintf("ERROR: Missing value --%s\nSee --help flag for usage", k.to_s)) if options[k].nil?
      end

      @options = options

      @options[:miniidb_port] = options[:miniidb_port].nil? ? 8080 : options[:miniidb_port].to_i
      @options[:idb_port]     = options[:idb_port].nil? ? 443 : options[:idb_port].to_i

      raise ArgumentError.new("ERROR: Use --key and --cert flags for client authentication to iDB\nSee --help flag for usage") if @options[:key].nil? ^ @options[:cert].nil?

      @exit_code = 0

      @logger       = Logger.new(sprintf('synclog.%s', Time.now.to_i), 1, 1024000)
      @logger.level = Logger::INFO
    end

    def sync
      clean_idb_switch_records # For idempotencies sake, delete first then recreate
      sync_fabrics
      sync_san_switches
      sync_resource('datacenter',
                    sprintf('/datacenters?name=%s', @options[:datacenter]),
                    sprintf('/datacenters?name=%s', @options[:datacenter]),
                    @options[:datacenter])
      exit @exit_code
    end

    def clean_idb_switch_records
      # Will be recreated shortly
      san_switches = request('GET',
                             @options[:idb_endpoint],
                             @options[:idb_port],
                             '/sanswitches')
      san_switches.each do |san_switch|
        delete_idb_record('sanswitch', san_switch['id'])
      end

      fabrics = request('GET',
                        @options[:idb_endpoint],
                        @options[:idb_port],
                        '/fabrics')

      fabrics.each do |fabric|
        delete_idb_record('fabric', fabric['id'])
      end

    end

    def sync_fabrics
      miniidb_fabrics = request('GET',
                                @options[:miniidb_host],
                                @options[:miniidb_port],
                                '/fabrics')

      miniidb_fabrics.each do |fabric|
        create_idb_record('fabric',
                          fabric['name'],
                          fabric)
      end
    end

    def sync_san_switches

      miniidb_san_switches = request('GET',
                                     @options[:miniidb_host],
                                     @options[:miniidb_port],
                                     '/sanswitches')

      miniidb_san_switches.each do |san_switch|
        # Some records have a Fabric value of a hash, some are strings..
        fabric_name = ''
        if san_switch['fabric'].is_a? Hash
          fabric_name = san_switch['fabric']['name']

        else
          # If fabric value is a string..
          fabric_name = request('GET',
                                @options[:miniidb_host],
                                @options[:miniidb_port],
                                sprintf('/sanswitches?id=%s&fields=fabric.name', san_switch['id']))[0]['fabric']['name']
        end
        idb_fabric = request('GET',
                             @options[:idb_endpoint],
                             @options[:idb_port],
                             sprintf('/fabrics?name=%s', fabric_name))[0]
        san_switch.delete('fabric')

        parent_info = {'fabric' => {'id'            => idb_fabric['id'],
                                      'versionNumber' => idb_fabric['versionNumber']}}
        # First have to create the record without ports, then add ports
        # Otherwise we get a optimistic locking failure
        create_san_switch = san_switch.clone
        create_san_switch['ports'] = []
        create_idb_record('sanswitch',
                          create_san_switch['name'],
                          create_san_switch,
                          parent_info)

        # Gross but otherwise next GET might return old info
        sleep 1
        idb_san_switch = request('GET',
                                 @options[:idb_endpoint],
                                 @options[:idb_port],
                                 sprintf('/sanswitches?name=%s', san_switch['name']))[0]

        idb_san_switch.delete('fabric')
        update_idb_record('sanswitch',
                          idb_san_switch['name'],
                          san_switch,
                          idb_san_switch,
                          parent_info)
      end

    end

    def sync_resource(type,
                      miniidb_path,
                      idb_path,
                      name,
                      parent_info = {})

      miniidb_records = request('GET',
                                @options[:miniidb_host],
                                @options[:miniidb_port],
                                miniidb_path)

      if miniidb_records.empty?
        warning(sprintf('Skipping - No %s records for %s found in %s [%s]',
                        type,
                        name,
                        @options[:miniidb_host],
                        miniidb_path))
        return
      end

      miniidb_records.each do |r|
        keys_to_ignore = ['versionNumber', 'createdDate', 'modifiedDate']
        miniidb_record = recursive_key_delete(r, keys_to_ignore)
        miniidb_id     = miniidb_record['id']
        idb_record = request('GET',
                             @options[:idb_endpoint],
                             @options[:idb_port],
                             idb_path)

        if idb_record.empty?
          if type == 'cluster'
            # Get duplicate value errors if we update everything at the same time
            # First update record minus clusterConfigs and deviceRoleConfigs, then add the config values
            two_step_create_idb_record(type,
                                       name,
                                       miniidb_record,
                                       parent_info,
                                       idb_path,
                                       miniidb_path,
                                       {'clusterConfigs'    => [],
                                        'deviceRoleConfigs' => []})
          elsif type == 'unmanagedhost' or type == 'host'
            # Don't update with entire cluster record and can't create hostConfigs & networkInterfaces at the same time as (unmanaged)host record
            # First create record then update with hostConfigs & networkInterfaces
            unless name.nil? and type == 'host'
              # Null host records will be copied over in copy_clusters_null_hosts
              two_step_create_idb_record(type,
                                         name,
                                         miniidb_record,
                                         parent_info,
                                         idb_path,
                                         miniidb_path,
                                         {'hostConfigs'          => [],
                                          'networkInterfaces'    => [],
                                          'hbaPorts'             => [],
                                          'memoryStorageDevices' => [],
                                          'storageDevices'       => []},
                                         ['cluster'])
            end
          else
            create_idb_record(type,
                              name,
                              miniidb_record,
                              parent_info)
          end
        elsif idb_record.length == 1
          if type == 'cluster'
            # Prevent duplication of null hosts
            delete_null_host_records(idb_record[0]['id'])
            # Get duplicate value errors if we update everything at the same time
            # First update record minus clusterConfigs and deviceRoleConfigs, then add the config values
            two_step_update_idb_record(type,
                                       name,
                                       miniidb_record,
                                       idb_record[0],
                                       parent_info,
                                       idb_path,
                                       miniidb_path,
                                       {'clusterConfigs' => [],
                                        'deviceRoleConfigs' => []})

          elsif type == 'unmanagedhost' or type == 'host'
            # Null host records will be copied over in copy_clusters_null_hosts
            unless miniidb_record['name'].nil? and type == 'host'
              #First update record, then update hostConfigs & networkInteraces
              two_step_update_idb_record(type,
                                         name,
                                         miniidb_record,
                                         idb_record[0],
                                         parent_info,
                                         idb_path,
                                         miniidb_path,
                                         {'hostConfigs'          => [],
                                          'networkInterfaces'    => [],
                                          'hbaPorts'             => [],
                                          'memoryStorageDevices' => [],
                                          'storageDevices'       => []},
                                         ['cluster'])
            end

          else
            update_idb_record(type,
                              name,
                              miniidb_record,
                              idb_record[0])
          end
        else
           warning(sprintf('Skipping - multiple %s records for %s found in %s [%s]',
                           type,
                           name,
                           @options[:idb_endpoint],
                           idb_path)) unless type == 'host' and name.nil? #null hosts records are handling in copy_clusters_null_hosts
          return
        end

        updated_record = request('GET',
                                 @options[:idb_endpoint],
                                 @options[:idb_port],
                                 idb_path)[0]

        return if LAST_LEVELS.include?(type)

        child_types = CHILDREN_MAPPING[type]

        identifier = (type == 'datacenter') ? name : miniidb_id
        new_parent_type = (type == 'datacenter') ? 'dataCenter' : type

        new_parent_info = { new_parent_type => { 'id'            => updated_record['id'],
                                                 'versionNumber' => updated_record['versionNumber'] }}

        copy_clusters_null_hosts(miniidb_id, new_parent_info) if type == 'cluster'
        child_types.each do |child_type|
          children = get_children(type,
                                  identifier,
                                  child_type)
          children.each do |child|
            child_name = child['name']
            child_name_path = child_name.nil? ? '' : sprintf('name=%s&', child_name) # Record names can be nil

            # Blame the english language
            child_resource_path = child_type == 'sanswitch' ? 'sanswitches' : sprintf('%ss', child_type)
            miniidb_child_path = sprintf('/%s?%s%s.id=%s',
                                         child_resource_path,
                                         child_name_path,
                                         new_parent_type,
                                         miniidb_id)

            idb_child_path = sprintf('/%s?%s%s.id=%s',
                                     child_resource_path,
                                     child_name_path,
                                     new_parent_type,
                                     updated_record['id'])

            sync_resource(child_type,
                          miniidb_child_path,
                          idb_child_path,
                          child_name,
                          new_parent_info)
          end
        end
      end
    end

    def copy_clusters_null_hosts(miniidb_cluster_id, parent_info)
      hosts = request('GET',
                      @options[:miniidb_host],
                      @options[:miniidb_port],
                      sprintf('/clusters?id=%s&expand=hosts&fields=hosts', miniidb_cluster_id))[0]['hosts']

      hosts.each do |host|
        if host['name'].nil?
          create_null_name_host_record(host, parent_info)
        end
      end
    end

    def create_null_name_host_record(miniidb_record,
                                     parent_info)
      say(sprintf('Creating host new record with a null hostname in %s', @options[:idb_endpoint]))

      first_update = miniidb_record
      first_update.merge!(parent_info)
      first_update.delete('versionNumber')
      first_update = recursive_value_delete(first_update, [nil])

      # There can be added once the host record is created
      {'hostConfigs'          => [],
       'networkInterfaces'    => [],
       'hbaPorts'             => [],
       'memoryStorageDevices' => [],
       'storageDevices'       => []}.each do |k, v|
        first_update[k] = v
      end

      response = request('POST',
                         @options[:idb_endpoint],
                         @options[:idb_port],
                         '/hosts',
                         first_update.to_s)[0]

      #Gross but otherwise next GET might return old data
      sleep 1

      idb_record = request('GET',
                           @options[:idb_endpoint],
                           @options[:idb_port],
                           sprintf('/hosts/%s', response['id']))[0]

      update_idb_record('host',
                        nil,
                        miniidb_record,
                        idb_record,
                        parent_info)

      say(sprintf('Created new host record with null name in %s with an id of %s',
                  @options[:idb_endpoint],
                  idb_record['id']))
    end

    def delete_null_host_records(id)
      # Deletes null host records for a given cluster id
      # This is to prevent duplication of null host records when a cluster is updated
      # If a host still has a null name, the record will be recreated
      # If the host has since been assigned a name, a new record will be created
      hosts = request('GET',
                      @options[:idb_endpoint],
                      @options[:idb_port],
                      sprintf('/clusters?id=%s&expand=hosts&fields=hosts.name,hosts.id', id))[0]['hosts']

      hosts.each do |host|
        if host['name'].nil?
          delete_idb_record('host', host['id'])
        end
      end
    end

    def delete_idb_record(type, id)
      say(sprintf('Deleting %s record with an id of %s in %s',
                  type,
                  id,
                  @options[:idb_endpoint]))

      type_path = type == 'sanswitch' ? 'sanswitches' : sprintf('%ss', type)
      request('DELETE',
              @options[:idb_endpoint],
              @options[:idb_port],
              sprintf('/%s/%s', type_path, id))
    end

    def get_children(type,
                     id,
                     child_type)

      if type == 'datacenter'
        return request('GET',
                       @options[:miniidb_host],
                       @options[:miniidb_port],
                       sprintf('/datacenters?name=%s&expand=superpods&fields=superpods.name', id))[0]['superpods']
      elsif type == 'superpod'
        return request('GET',
                       @options[:miniidb_host],
                       @options[:miniidb_port],
                       sprintf('/superpods?id=%s&expand=clusters&fields=clusters.name', id))[0]['clusters']
      elsif type == 'cluster'
        if child_type == 'host'
          return request('GET',
                         @options[:miniidb_host],
                         @options[:miniidb_port],
                         sprintf('/clusters?id=%s&expand=hosts&fields=hosts.name', id))[0]['hosts']
        elsif child_type == 'unmanagedhost'
          return request('GET',
                         @options[:miniidb_host],
                         @options[:miniidb_port],
                         sprintf('/clusters?id=%s&expand=unmanagedHosts&fields=unmanagedHosts.name', id))[0]['unmanagedHosts']
        else
          bailout(sprintf('Unknown child type passed for %s %s: %s',
                          type,
                          name,
                          child_type))
        end
      else
        bailout(sprintf('Cannot get children for unknown type %s', type))
      end

    end

    def create_idb_record(type,
                          name,
                          data,
                          parent_info = {})
      say(sprintf('Creating new %s record for %s in %s',
                  type,
                  name,
                  @options[:idb_endpoint]))

      data.merge!(parent_info)
      data.delete('versionNumber')
      data = recursive_value_delete(data, [nil])

      #Blame the english language
      type_path = type == 'sanswitch' ? '/sanswitches' : sprintf('/%ss', type)
      request('POST',
              @options[:idb_endpoint],
              @options[:idb_port],
              type_path,
              data.to_s)
    end

    def two_step_create_idb_record(type,
                                   name,
                                   miniidb_record,
                                   parent_info,
                                   idb_path,
                                   miniidb_path,
                                   special_fields = {},
                                   fields_to_ignore = [])

      # special fields can't be updated at the same time as the rest of the record
      # first update record, then add special fields
      special_fields.each do |f, v|
        miniidb_record[f] = v if miniidb_record.key?(f)
      end

      fields_to_ignore.each do |f|
        miniidb_record.delete(f)
      end

      create_idb_record(type,
                        name,
                        miniidb_record,
                        parent_info)

      #Gross but otherwise somtimes the next GET will return old data
      sleep 1

      new_idb_record = request('GET',
                               @options[:idb_endpoint],
                               @options[:idb_port],
                               idb_path)[0]

      miniidb_record = request('GET',
                               @options[:miniidb_host],
                               @options[:miniidb_port],
                               miniidb_path)[0]

      fields_to_ignore.each do |f|
        new_idb_record.delete(f)
        miniidb_record.delete(f)
      end

      update_idb_record(type,
                        name,
                        miniidb_record,
                        new_idb_record,
                        parent_info)
    end

    def update_idb_record(type,
                          name,
                          miniidb_record,
                          idb_record,
                          parent_info = {})

      say(sprintf('Updating existing %s record for %s in %s',
                  type,
                  name,
                  @options[:idb_endpoint]))

      new_record = miniidb_record
      new_record['id'] = idb_record['id']
      new_record['versionNumber'] = idb_record['versionNumber']
      new_record.merge!(parent_info)

      new_record = recursive_value_delete(new_record, [nil])
      #blame the english language
      type_path = type == 'sanswitch' ? 'sanswitches' : sprintf('%ss', type)
      request('PUT',
              @options[:idb_endpoint],
              @options[:idb_port],
              sprintf('/%s/%s', type_path, new_record['id']),
              new_record.to_s)
    end

    def two_step_update_idb_record(type,
                                   name,
                                   miniidb_record,
                                   idb_record,
                                   parent_info,
                                   idb_path,
                                   miniidb_path,
                                   special_fields = {},
                                   fields_to_ignore = [])

      # special fields can't be updated at the same time as the rest of the record
      # first update record, then add special fields
      fields_to_ignore.each do |f|
        idb_record.delete(f)
        miniidb_record.delete(f)
      end

      special_fields.each do |f, v|
        idb_record[f] = v if idb_record.key?(f)
        miniidb_record[f] = v if miniidb_record.key?(f)
      end

      update_idb_record(type,
                        name,
                        miniidb_record,
                        idb_record,
                        parent_info)

      #Gross but otherwise sometimes the next GET will return old data
      sleep 1

      idb_record = request('GET',
                           @options[:idb_endpoint],
                           @options[:idb_port],
                           idb_path)[0]

      miniidb_record = request('GET',
                               @options[:miniidb_host],
                               @options[:miniidb_port],
                               miniidb_path)[0]

      fields_to_ignore.each do |f|
        idb_record.delete(f)
        miniidb_record.delete(f)
      end

      update_idb_record(type,
                        name,
                        miniidb_record,
                        idb_record,
                        parent_info)
    end


    def request(request,
                host,
                port,
                path,
                body = nil)

      path     = URI.escape(path)
      http     = Net::HTTP.new(host, port)
      protocol = 'http'

      if host == @options[:idb_endpoint]
        #TODO update when we figure out what we're doing for miniidb auth
        if @options[:idb_port].to_i == 443
          protocol     = 'https'
          http.use_ssl = true
        end

        if @options[:key] and @options[:cert]
          http.cert    = OpenSSL::X509::Certificate.new(File.read @options[:cert])
          http.key     = OpenSSL::PKey::RSA.new(File.read @options[:key])
        end
      end


      begin
        response = http.send_request(request.upcase,
                                     sprintf('%s%s', API_VERSION, path),
                                     body, {'Content-Type' => 'application/json'})
      rescue => e
        bailout(sprintf('%s request failed: [%s://%s:%s%s%s]: %s',
                        request.upcase,
                        protocol,
                        host,
                        port,
                        API_VERSION,
                        path,
                        e.message))
      end

      response_body = JSON.parse(response.body)

      warning(sprintf("%s request was unsuccessful [%s://%s:%s%s%s]: %s\n%s",
                      request.upcase,
                      protocol,
                      host,
                      port,
                      API_VERSION,
                      path,
                      response_body['message'],
                      body)) unless response_body['success']

      response_body['data']
    end

    def recursive_key_delete(hash, keys_to_remove)
      keys_to_remove.each do |key|
        hash.delete(key)
      end
      hash.each_value do |value|
        if value.is_a? Hash
          recursive_key_delete(value, keys_to_remove) if value.is_a? Hash
        elsif value.is_a? Array
          value.each do |i|
            recursive_key_delete(i, keys_to_remove) if value.is_a? Hash
          end
        end
      end
    end

    def recursive_value_delete(hash, values_to_remove)
      hash.delete_if { |key, value| values_to_remove.include?(value) }
      hash.each_value do |value|
        if value.is_a? Hash
          recursive_value_delete(value, values_to_remove)
        elsif value.is_a? Array
          value.each do |i|
            recursive_value_delete(i, values_to_remove) if i.is_a? Hash
          end
        end
      end
    end

    def say(msg)
      @logger.info(msg)
      puts msg
    end

    def warning(msg)
      @logger.warn(msg)
      warn(msg)
      @exit_code = 1
    end

    def bailout(msg)
      @logger.fatal(msg)
      raise msg
    end
  end
end

if $0 == __FILE__
  options = {}
  parser = OptionParser.new do |o|
    o.banner = 'USAGE: ./sync.rb --miniidb_host HOST --idb_endpoint EP --datacenter DC [--miniidb_port PORT] [--idb_port PORT] [--key KEY] [--cert CERT]'
    o.on('--miniidb_host HOST', 'REQUIRED: miniidb host')                                        { |v| options[:miniidb_host] = v }
    o.on('--idb_endpoint EP',   'REQUIRED: idb endpoint')                                        { |v| options[:idb_endpoint] = v }
    o.on('--datacenter DC',     'REQUIRED: data center to copy')                                 { |v| options[:datacenter] = v }
    o.on('--miniidb_port PORT', 'OPTIONAL: miniidb port, defaults to 8080')                      { |v| options[:miniidb_port] = v.to_i }
    o.on('--idb_port PORT',     'OPTIONAL: idb port, defaults to 443')                           { |v| options[:idb_port] = v.to_i }
    o.on('--key KEY',           'OPTIONAL: key used for client authentication to iDB endpoint')  { |v| options[:key] = v }
    o.on('--cert CERT',         'OPTIONAL: cert used for client authentication to iDB endpoint') { |v| options[:cert] = v }
  end
  parser.parse!
  @syncer = MiniIDB::Sync.new(options)
  @syncer.sync
end
