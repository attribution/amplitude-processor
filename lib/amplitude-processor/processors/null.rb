module AmplitudeProcessor
  module Processors
    class Null
      def track(attrs)
        true
      end

      def identify(attrs)
        true
      end

      def page(attrs)
        true
      end

      def flush
        true
      end
    end
  end
end
