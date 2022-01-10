require File.expand_path('../lib/amplitude-rs3-segment/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ['Maxim Sadovsky']
  gem.email         = ['maksim@attributionapp.com']
  gem.summary       = 'Amplitude Retrospective S3 to Segment processor'
  gem.description   = 'Reads Amplitude Retrospective S3 Syncs from AWS S3 buckets and process it as Segment events'
  gem.homepage      = 'https://attributionapp.com'
  gem.license       = 'MIT'

  gem.executables   = ['amplitude-rs3-segment']
  gem.files         = `git ls-files | grep -Ev '^(test|myapp|examples)'`.split("\n")
  gem.test_files    = []
  gem.name          = 'amplitude-rs3-segment'
  gem.require_paths = ['lib']
  gem.version       = AmplitudeRS3Segment::VERSION
  gem.required_ruby_version = '>= 2.2.2'

  gem.add_dependency 'avro'
  gem.add_dependency 'snappy'
  gem.add_dependency 'aws-sdk-s3', '~> 1'
  gem.add_dependency 'analytics-ruby', '~> 2.0'
  gem.add_dependency 'activesupport'

  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'dotenv'
end
