module AmplitudeProcessor
  module Senders
    class Console
      def track(attrs)
        puts '====================================='
        puts '=== Track:'
        pp attrs
      end

      def identify(attrs)
        puts '====================================='
        puts '=== Identify:'
        pp attrs
      end

      def page(attrs)
        puts '====================================='
        puts '=== Page:'
        pp attrs
      end

      def flush
        puts '====================================='
        puts '=== Flush:'
      end
    end
  end
end
