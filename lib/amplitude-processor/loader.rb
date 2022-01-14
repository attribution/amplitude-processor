require 'aws-sdk-s3'
require 'active_support'
require 'active_support/time'

module AmplitudeProcessor
  class Loader
    AWS_S3_DEFAULT_REGION = 'us-east-1'.freeze
    UTC_TIMEZONE = Time.find_zone('UTC').freeze
    FILE_REGEXP = /.+\.json.gz$/.freeze

    attr_accessor :processor, :project_identifier, :aws_s3_bucket, :prompt, :process_single_sync, :skip_before

    def initialize(processor, project_identifier, aws_s3_bucket, aws_access_key_id, aws_secret_access_key, s3_dir='', aws_region=nil)
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
      @s3_dir = s3_dir
      @s3_dir += '/' unless @s3_dir.end_with?('/')

      @prompt = false
      @process_single_sync = false # stops after one sync is processed
      @skip_before = nil
    end

    def call
      scan_files.each do |obj|
        already_imported = begin
          @s3.head_object({ bucket: @aws_s3_bucket, key: "#{@s3_dir}imported/#{File.basename(obj.key)}" }) && true
        rescue Aws::S3::Errors::NotFound
          nil
        end

        next if already_imported

        if @prompt
          require 'pry'
          logger.debug 'Ready to process ' + obj.key + ', type "exit!" to interrupt, "already_synced = true" to skip this sync, set @skip_types to skip certian event types and CTRL-D to continue'
          binding.pry

          next if already_imported
        end

        process_file(obj)
        mark_file_as_imported(obj)
        true
        break if @process_single_sync
      end
    end

    def logger
      AmplitudeProcessor.logger
    end

    def scan_files
      list_opts = { bucket: @aws_s3_bucket, prefix: @s3_dir, delimiter: '/' }
      all_objects = []
      while (resp = @s3.list_objects_v2(list_opts)).next_continuation_token.present?
        all_objects += resp.contents
        list_opts[:continuation_token] = resp.next_continuation_token
      end
      all_objects.select { |obj| obj.key.match(FILE_REGEXP) }.sort_by(&:key)
    end

    def mark_file_as_imported(obj)
      @s3.put_object(
        bucket: @aws_s3_bucket,
        key: "#{@s3_dir}imported/#{File.basename(obj.key)}",
        body: ''
      )
    end

    def process_file(obj)
      logger.info "Processing file(#{obj.key})"

      load_start_time = Time.now.utc
      file_obj = @s3.get_object({ bucket: @aws_s3_bucket, key: obj.key })
      reader = Zlib::GzipReader.new(file_obj.body)
      load_diff = Time.now.utc - load_start_time

      counter = 0
      skipped = 0
      start_time = Time.now.utc # we start timer after file is read from S3

      reader.each_line do |line|
        # TODO sample raw logger
        # if counter % 10_000 == 0
        # if counter == 0
        #   logger.info hash.inspect
        # end
        hash = JSON.parse(line)
        next if hash.empty?

        identify(hash) if hash['user_properties'].present?

        result = case hash['event_type']
        when 'Loaded a Page', /^Viewed .* Page$/
          page(hash)
        else
          track(hash)
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
      UTC_TIMEZONE.parse(time).utc
    end

    def wrap_cookie(amplitude_id)
      amplitude_id ? "#{@project_identifier}|#{amplitude_id}" : nil
    end

    def common_payload(hash)
      amplitude_id = hash['amplitude_id']
      {
        anonymous_id: wrap_cookie(amplitude_id),
        message_id: "AMPLITUDE|#{hash['event_id']}",
        timestamp: parse_time(hash['event_time']),
        context: {
          'ip' => hash['ip_address'],
          'library' => {
            'name' => 'AmplitudeIntegration',
            'version' => VERSION
          }
        },
        properties: {
          'amplitude_user_id' => amplitude_id
        }
      }
    end

    def skip_before?(timestamp)
      @skip_before && timestamp < @skip_before
    end

    def track(hash)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:event] = hash['event_type']
      payload[:properties].merge!(hash['event_properties'].reject { |_, v| v.nil? || v.to_s.bytesize > 200 })
      payload[:user_id] = hash['user_id'] if hash['user_id']

      @processor.track(payload)
    end

    def page(hash)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:name] = hash['event_type']
      payload[:user_id] = hash['user_id'] if hash['user_id']

      payload[:properties] = hash['event_properties']
      @processor.page(payload)
    end

    def identify(hash)
      payload = common_payload(hash)
      return if skip_before?(payload[:timestamp])

      payload[:user_id] = hash['user_id'] if hash['user_id']
      payload[:traits] = hash['user_properties']

      raise 'user_id or anonymous_id must be present' if payload[:user_id].nil? && payload[:anonymous_id].nil?

      @processor.identify(payload)
    end
  end
end
