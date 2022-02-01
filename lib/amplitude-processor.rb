require 'amplitude-processor/logging'
require 'amplitude-processor/loader'
require 'amplitude-processor/senders/console'
require 'amplitude-processor/senders/null'

module AmplitudeProcessor
  def self.logger
    AmplitudeProcessor::Logging.logger
  end

  def self.logger=(log)
    AmplitudeProcessor::Logging.logger = log
  end
end
