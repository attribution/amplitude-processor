module AmplitudeProcessor
  module Senders
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
