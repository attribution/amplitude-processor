This gem loads Amplitude data from S3 and:
1. Translates events to Segment spec
2. Calls extra `sender.identify` if an event has user info
3. Calls `sender.alias` if current file is a "merged users" file
4. Calls `sender.track` for each event
5. Flushes `sender` after all data is processed

Available senders:
1. `NullSender` - does nothing
2. `ConsoleSender` - prints all data to console, useful for debugging
3. `BulkEventSender` - resides in AUX and actually sends Segment-compatibe data to Attribution

Each file is imported only once: after we imported file `XXX`, we create a new file `imported/XXX` and skip `XXX` on the next run. 

### Test run:

```ruby
require 'bundler/setup'
require 'amplitude-processor'
require 'amplitude-processor/loader'
require 'amplitude-processor/senders/console'

sender = AmplitudeProcessor::Senders::Console.new
AmplitudeProcessor::Loader.new(
  sender,
  'PROJECT_ID',
  'attribution-amplitude-test', # S3 bucket
  ENV['AWS_ACCESS_KEY_ID'],
  ENV['AWS_SECRET_ACCESS_KEY'],
  'test_1/358080/' # S3 directory where *.json.gz files are stored
).call
```
