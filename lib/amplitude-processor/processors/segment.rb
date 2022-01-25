require 'segment/analytics'

module AmplitudeProcessor
  module Processors
    class Segment
      attr_accessor :analytics, :max_queue_size

      def initialize(*args)
        @analytics = ::Segment::Analytics.new(*args)

        @max_queue_size = @analytics.
          instance_variable_get('@client').
          instance_variable_get('@max_queue_size')
      end

      def check_flush_queue!
        if @analytics.queued_messages >= @max_queue_size
          t = Time.now
          AmplitudeProcessor.logger.info "Max queue size reached - #{@analytics.queued_messages}, flushing"
          @analytics.flush
          diff = Time.now - t
          rate = (@max_queue_size / diff).to_i
          AmplitudeProcessor.logger.info "Flush done in #{diff} seconds (#{rate} req/sec), continue"
        end
      end

      def track(attrs)
        check_flush_queue!
        @analytics.track(attrs)
      end

      def identify(attrs)
        check_flush_queue!
        @analytics.identify(attrs)
      end

      def page(attrs)
        check_flush_queue!
        @analytics.page(attrs)
      end

      def flush
        @analytics.flush
      end
    end
  end
end
