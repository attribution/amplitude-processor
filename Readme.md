### Test run:

```ruby
require 'bundler/setup'
require 'amplitude-processor'
require 'amplitude-processor/loader'
require 'amplitude-processor/senders/console'

processor = AmplitudeProcessor::Senders::Console.new
AmplitudeProcessor::Loader.new(
  sender,
  'project_id',
  'attribution-amplitude-test',
  ENV['AWS_ACCESS_KEY_ID'],
  ENV['AWS_SECRET_ACCESS_KEY'],
  'test_1/358080/'
).call
```
