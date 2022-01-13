require 'aws-sdk-s3'
require 'avro'
require 'active_support/time'

module AmplitudeProcessor
  class Loader
    AWS_S3_DEFAULT_REGION = 'us-east-1'

    attr_accessor :processor, :project_identifier, :aws_s3_bucket, :prompt, :process_single_sync, :skip_before

    def initialize(processor, project_identifier, aws_s3_bucket, aws_access_key_id, aws_secret_access_key, dir='', aws_region=nil)
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
      @skip_before = nil
    end

    def call
      scan_files.each do |obj|
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

        process_file(obj)
        break if @process_single_sync
      end
    end

    def logger
      AmplitudeProcessor.logger
    end

    def scan_files
      list_opts = { bucket: @aws_s3_bucket, prefix: @aws_s3_bucket_prefix, delimiter: '/' }
      resp = @s3.list_objects_v2(list_opts)
      resp.contents.select { |obj| obj.key.match(FILE_REGEXP) }.sort_by(&:key)
    end

    def mark_file_as_synced(obj)
      @s3.copy_object(
        copy_source: "#{@aws_s3_bucket}/#{obj.key}",
        bucket: @aws_s3_bucket,
        key: "imported_#{obj.key}"
      )
    end

    def process_file(file)
      logger.info "Processing file(#{file})"

      load_start_time = Time.now.utc
      s3_file = s3_get_file(file)
      reader = Avro::IO::DatumReader.new
      avro = Avro::DataFile::Reader.new(s3_file.body, reader)
      load_diff = Time.now.utc - load_start_time

      counter = 0
      skipped = 0
      start_time = Time.now.utc # we start timer after file is read from S3

      avro.each do |hash|
        # TODO sample raw logger
        # if counter % 10_000 == 0
        # if counter == 0
        #   logger.info hash.inspect
        # end

        result = case type
        when :track
          track(hash, event_name)
        when :alias
          store_alias(hash)
        else
          send(type, hash)
        end

        skipped += 1 if result.nil?
        counter += 1
      end

      diff = Time.now.utc - start_time
      if diff > 0
        logger.info "Done. Loading #{load_diff.to_i}s, processing #{diff.to_i}s, #{counter} rows, #{skipped} skipped (#{(counter / diff).to_i} rows/sec)"
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
        message_id: "AMPLITUDE|#{hash.delete('event_id')}",
        timestamp: parse_time(hash.delete('time')),
        context: {
          'ip' => (hash.delete('ip') || hash.delete('browser_ip')),
          'library' => {
            'name' => 'AmplitudeIntegration',
            'version' => '1.0'
          }
        },
        properties: {
          'heap_user_id' => heap_user_id
        }
      }
    end

    def skip_before?(timestamp)
      @skip_before && timestamp < @skip_before
    end

    def track(hash, event_name)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:event] = event_name
      payload[:properties].merge!(hash.reject { |_, v| v.nil? || v.to_s.bytesize > 200 })

      if revenue_field = @revenue_mapping[event_name]
        payload[:properties]['revenue'] ||= hash.delete(revenue_field.to_s)
      else @revenue_fallback.any?
        payload[:properties]['revenue'] ||= hash.values_at(*@revenue_fallback).compact.first
      end

      @processor.track(payload)
    end

    def page(hash)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:name] = 'Loaded a Page'

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
        'referrer' => hash.delete('previous_page') || hash.delete('referrer'),
        'title' => hash.delete('title'),
        'url' => url,
        'session_referrer' => hash.delete('referrer')
      }

      @processor.page(payload)
    end

    def identify(hash)
      heap_user_id = hash.delete('user_id')
      email = hash.delete('email') || hash.delete('_email')
      identity = hash.delete('identity')

      # common workaround for heap? email used as identity
      if email.nil? && identity && identity.include?('@')
        email = identity
      end

      # uses email as USER_ID or sets it to null (if email is empty)
      if @user_id_prop == 'email'
        identity = email
      end

      payload = {
        anonymous_id: wrap_cookie(heap_user_id),
        user_id: identity,
        traits: {
          'email' => email,
          'identity' => identity,
          'heap_user_id' => heap_user_id,
          'join_date' => parse_heap_timestamp(hash.delete('joindate')),
          'last_modified' => parse_heap_timestamp(hash.delete('last_modified'))
        }.reject { |_, v| v.nil? }
      }

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