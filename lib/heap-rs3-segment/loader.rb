require 'aws-sdk-s3'
require 'avro'
require 'active_support/time'

module HeapRS3Segment
  class Loader
    AWS_S3_DEFAULT_REGION = 'us-east-1'

    attr_accessor :processor, :project_identifier, :aws_s3_bucket, :prompt, :process_single_sync, :skip_types, :identify_only_users

    def initialize(processor, project_identifier, aws_s3_bucket, aws_access_key_id, aws_secret_access_key, aws_region=nil)
      Time.zone = 'UTC'
      @alias_cache = {}

      @processor = processor
      @project_identifier = project_identifier

      @s3 = Aws::S3::Client.new(
        access_key_id: aws_access_key_id,
        secret_access_key: aws_secret_access_key,
        region: aws_region || AWS_S3_DEFAULT_REGION
      )
      @aws_s3_bucket = aws_s3_bucket
      @aws_s3_bucket_prefix = MANIFEST_BUCKET_PREFIX

      @prompt = true
      @process_single_sync = true # stops after one sync is processed
      @skip_types = [] # [:page, :track, :identify, :alias]
      @identify_only_users = false # this is useful when doing initial import and we don't need to identify anonymous users
    end

    def call
      scan_manifests.each do |obj|
        already_synced = begin
          @s3.head_object({ bucket: @aws_s3_bucket, key: "imported_#{obj.key}" }) && true
        rescue Aws::S3::Errors::NotFound
          nil
        end

        next if already_synced

        if @prompt
          require 'pry'
          logger.debug 'Ready to process ' + obj.key + ', type "exit!" to interrupt, "already_synced = true" to skip this sync, set @skip_types to skip certian event types and CTRL-D to continue'
          binding.pry

          next if already_synced
        end

        process_sync(obj)
        break if @process_single_sync
      end
    end

    def logger
      HeapRS3Segment.logger
    end

    def scan_manifests
      list_opts = { bucket: @aws_s3_bucket, prefix: @aws_s3_bucket_prefix, delimiter: '/' }
      resp = @s3.list_objects_v2(list_opts)
      resp.contents.select { |obj| obj.key.match(MANIFEST_REGEXP) }.sort_by(&:key)
    end

    def mark_manifest_as_synced(obj)
      @s3.copy_object(
        copy_source: URI::encode("#{@aws_s3_bucket}/#{obj.key}"),
        bucket: @aws_s3_bucket,
        key: "imported_#{obj.key}"
      )
    end

    def process_sync(obj)
      manifest = get_manifest(obj)
      process_manifest(manifest)
      mark_manifest_as_synced(obj)
    end

    def get_manifest(obj)
      logger.info "Reading #{obj.key}"

      manifest = s3_get_file(obj)
      JSON.parse(manifest.body.read)
    end

    def process_manifest(manifest)
      logger.info "Processing manifest(dump_id: #{manifest['dump_id']})"

      # skip "sessions" processing - we don't need them
      tables = manifest['tables'].reject { |table| table['name'] == 'sessions'}

      # custom sorter - any events then pageviews, identify, aliases
      index_type_name = ->(table) {
        idx_type = case table['name']
        when 'user_migrations' then [1, :alias]
        when 'pageviews' then [3, :page]
        when 'users' then [4, :identify]
        else [2, :track]
        end
        idx_type << table['name']
      }
      tables.sort_by!(&index_type_name)

      tables.each do |table|
        table['type'] = index_type_name.call(table)[1]
        logger.info "Order key #{index_type_name.call(table)}"
      end

      tables.each do |table|
        process_table(table)
      end
    end

    def process_table(table)
      event_name = table['name'].split('_').map(&:capitalize).join(' ')
      logger.info "Processing table(#{table['name']}) - \"#{event_name}\" event"

      files = table['files'].sort

      if ['users', 'user_migrations'].include?(table['name'])
        files.sort_by! do |path|
          filename = path.split('/').last
          filename.split('_').first.to_i
        end
      end

      files.each do |file|
        next if @skip_types.include?(table['type'])
        process_file(file, table['type'], event_name)
      end
    end

    def process_file(file, type, event_name)
      logger.info "Processing file(#{file})"

      s3_file = s3_get_file(file)
      reader = Avro::IO::DatumReader.new
      avro = Avro::DataFile::Reader.new(s3_file.body, reader)

      counter = 0
      start_time = Time.now.utc # we start timer after file is read from S3

      avro.each do |hash|
        counter += 1
        case type
        when :track
          track(hash, event_name)
        when :alias
          store_alias(hash)
        else
          send(type, hash)
        end
      end

      diff = Time.now.utc - start_time
      if diff > 0
        logger.info "Done processing #{counter} events in #{diff.to_i} seconds (#{(counter / diff).to_i} req/sec)"
      end
    end

    def parse_time(time)
      Time.zone.parse(time).utc
    end
    
    def parse_heap_timestamp(value)
      return unless value
      Time.at(value / 1_000_000).utc.iso8601
    rescue
      value
    end

    def wrap_cookie(heap_user_id)
      heap_user_id ? "#{@project_identifier}|#{resolve_heap_user(heap_user_id)}" : nil
    end

    def common_payload(hash)
      heap_user_id = hash.delete('user_id')
      {
        anonymous_id: wrap_cookie(heap_user_id),
        message_id: "HEAP|#{hash.delete('event_id')}",
        timestamp: parse_time(hash.delete('time')),
        properties: {
          'heap_user_id' => heap_user_id
        }
      }
    end

    def track(hash, event_name)
      payload = common_payload(hash)

      payload[:event] = event_name
      payload[:properties].merge!(hash.reject { |_, v| v.nil? })

      if event_name == 'Shopify Confirmed Order'
        payload[:properties]['revenue'] = hash.delete('total_price')
      end
      
      if event_name == 'Exported Order'
        payload[:properties]['revenue'] = hash.delete('order_total')
      end

      @processor.track(payload)
    end

    def page(hash)
      payload = common_payload(hash)
      payload[:name] = 'Loaded a Page'
      payload[:context] = {
        'ip' => hash.delete('ip')
      }

      # TODO detect mobile and send screen event instead
      url = case hash['library']
      when 'web'
        'http://' + hash.values_at('domain', 'path', 'query', 'hash').join
      when 'ios', 'android'
        "#{hash['library']}-app://" + hash.values_at('app_name', 'view_controller').compact.join('/')
      else
        'unknown://' + hash['event_id']
      end

      payload[:properties] = {
        'referrer' => hash.delete('referrer'),
        'title' => hash.delete('title'),
        'url' => url
      }

      @processor.page(payload)
    end

    def identify(hash)
      heap_user_id = hash.delete('user_id')
      payload = {
        anonymous_id: wrap_cookie(heap_user_id),
        user_id: hash.delete('identity'),
        traits: {
          'email' => hash.delete('email') || hash.delete('_email'),
          'heap_user_id' => heap_user_id,
          'join_date' => parse_heap_timestamp(hash.delete('joindate')),
          'last_modified' => parse_heap_timestamp(hash.delete('last_modified'))
        }.reject { |_, v| v.nil? }
      }

      # common workaround for heap?
      if payload[:traits]['email'].nil?
        identity = payload[:user_id]

        if identity && identity.include?('@')
          payload[:traits]['email'] = identity
        end
      end

      payload[:traits] = hash.reject { |_, v| v.nil? }.merge(payload[:traits])

      return if @identify_only_users && payload[:user_id].nil?

      @processor.identify(payload)
    end

    def alias(hash)
      payload = {
        previous_id: wrap_cookie(hash['from_user_id']),
        anonymous_id: wrap_cookie(hash['to_user_id'])
      }

      @processor.alias(payload)
    end

    def store_alias(hash)
      @alias_cache[hash['from_user_id']] = hash['to_user_id']
    end

    def resolve_heap_user(heap_user_id)
      @alias_cache[heap_user_id] || heap_user_id
    end

    def s3uri_to_hash(s3uri)
      raise ArgumentError unless s3uri[0..4] == 's3://'

      bucket, key = s3uri[5..-1].split('/', 2)
      { bucket: bucket, key: key }
    end

    def s3_get_file(obj)
      hash = case obj
      when String
        s3uri_to_hash(obj)
      when Aws::S3::Types::Object
        { bucket: @aws_s3_bucket, key: obj.key }
      when Hash
        obj
      else
        {}
      end

      raise ArgumentError unless hash.has_key?(:bucket) && hash.has_key?(:key)

      @s3.get_object(hash)
    end

  end
end