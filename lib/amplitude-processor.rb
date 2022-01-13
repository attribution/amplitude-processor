require 'amplitude-processor/logging'
require 'amplitude-processor/loader'
require 'amplitude-processor/processors/segment'

module AmplitudeProcessor
  FILE_REGEXP = /.+\.json.gz$/.freeze
  MANIFEST_BUCKET_PREFIX = 'manifests/sync_'.freeze

  def self.logger
    AmplitudeProcessor::Logging.logger
  end

  def self.logger=(log)
    AmplitudeProcessor::Logging.logger = log
  end
end
