require 'amplitude-rs3-segment/logging'
require 'amplitude-rs3-segment/loader'
require 'amplitude-rs3-segment/processors/segment'

module AmplitudeRS3Segment
  MANIFEST_REGEXP = /\/sync_\d+\.json$/
  MANIFEST_BUCKET_PREFIX = 'manifests/sync_'

  def self.logger
    AmplitudeRS3Segment::Logging.logger
  end

  def self.logger=(log)
    AmplitudeRS3Segment::Logging.logger = log
  end
end
