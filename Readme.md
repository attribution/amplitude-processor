### Test run:

```ruby
require 'amplitude-processor'
require 'amplitude-processor/loader'
require 'amplitude-processor/processors/console'

processor = AmplitudeProcessor::Processors::Console.new
AmplitudeProcessor::Loader.new(
  processor,
  'project_id',
  'attribution-amplitude-test',
  ENV['AWS_ACCESS_KEY_ID'],
  ENV['AWS_SECRET_ACCESS_KEY'],
  'test_1/358080/'
).call
```
