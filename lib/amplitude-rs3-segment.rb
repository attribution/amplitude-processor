require 'amplitude-rs3-segment/logging'
require 'amplitude-rs3-segment/loader'
require 'amplitude-rs3-segment/processors/segment'

module AmplitudeRS3Segment
  FILE_REGEXP = /.+\.json.gz$/.freeze
  MANIFEST_BUCKET_PREFIX = 'manifests/sync_'.freeze

  def self.logger
    AmplitudeRS3Segment::Logging.logger
  end

  def self.logger=(log)
    AmplitudeRS3Segment::Logging.logger = log
  end
end
