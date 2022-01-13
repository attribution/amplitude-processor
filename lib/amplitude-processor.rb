require 'amplitude-processor/logging'
require 'amplitude-processor/loader'
require 'amplitude-processor/processors/segment'
require 'amplitude-processor/processors/console'
require 'amplitude-processor/processors/null'

module AmplitudeProcessor
  def self.logger
    AmplitudeProcessor::Logging.logger
  end

  def self.logger=(log)
    AmplitudeProcessor::Logging.logger = log
  end
end
