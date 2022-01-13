require File.expand_path('../lib/amplitude-processor/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ['Maxim Sadovsky']
  gem.email         = ['maksim@attributionapp.com']
  gem.summary       = 'Amplitude S3 processor'
  gem.description   = 'Read Amplitude S3 Syncs from AWS S3 buckets and process it as Segment events'
  gem.homepage      = 'https://attributionapp.com'
  gem.license       = 'MIT'

  gem.executables   = ['amplitude-processor']
  gem.files         = `git ls-files | grep -Ev '^(test|myapp|examples)'`.split("\n")
  gem.test_files    = []
  gem.name          = 'amplitude-processor'
  gem.require_paths = ['lib']
  gem.version       = AmplitudeProcessor::VERSION
  gem.required_ruby_version = '>= 2.2.2'

  gem.add_dependency 'aws-sdk-s3', '~> 1'
  gem.add_dependency 'analytics-ruby', '~> 2.0'
  gem.add_dependency 'activesupport'

  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'dotenv'
  gem.add_development_dependency 'rspec'
end
