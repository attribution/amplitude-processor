require 'spec_helper'

describe AmplitudeProcessor::Loader do
  let(:loader) { described_class.new(processor, 'project_identifier', 'aws_s3_bucket', 'aws_access_key_id', 'aws_secret_access_key') }
  let(:processor) { instance_double(AmplitudeProcessor::Processors::Segment) }

  before do
    allow(Aws::S3::Client).to receive(:new)
  end

  describe '#identify' do
    subject { loader.identify(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/with_user_properties.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"project_identifier|343954024786",
      :message_id=>"AMPLITUDE|740920720",
      :timestamp=>Time.parse('2022-01-11 10:00:28.675000000 +0000'),
      :context=>{
        "ip"=>nil,
        "library"=>{
          "name"=>"AmplitudeIntegration",
          "version"=>"0.1.0"
        }},
      :properties=>{"amplitude_user_id"=>343954024786},
      :user_id=>"4174",
      :traits=>{
        "current_period_spend"=>403452,
        "subscription_plan"=>"B2B Starter",
        "unsubscribe_token"=>
            "eyJhbGciOiJIUzI1NiJ9.eyJ1bnN1YnNjcmliZV9hY2NvdW50X2lkIjo0MTc0fQ.Vt3_P2WqCd-Ah-eTY6XCUdqsySBTTtsNiSIL4TjPrg8",
        "own_projects"=>"Imagine Halo",
        "unsubscribed"=>"false",
        "has_active_projects"=>true,
        "company_name"=>"Imagine Halo",
        "company_id"=>4174,
        "signup_method"=>"atb",
        "created_at"=>"2021-05-26T13:36:55Z",
        "subscription_status"=>"active",
        "email"=>"tom.petley@imaginehalo.com",
        "id"=>"4174"}
    } }

    it 'sends identify' do
      expect(processor).to receive(:identify).with(expected_payload)
      subject
    end
  end

  describe '#page' do
    subject { loader.page(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/page.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"project_identifier|346630956846",
      :message_id=>"AMPLITUDE|963956373",
      :timestamp=>Time.parse('2022-01-11 10:02:23.085000000 +0000'),
      :context=> {
        "ip"=>"172.226.50.20",
        "library"=>{
          "name"=>"AmplitudeIntegration",
          "version"=>"0.1.0"
        }
      },
      :properties=>{
        "url"=>"https://www.attributionapp.com/privacy/",
        "title"=>"Privacy - Attribution",
        "referrer"=>"https://www.google.ru/",
        "path"=>"/privacy/"
      },
      :name=>nil
    } }

    it 'sends page' do
      expect(processor).to receive(:page).with(expected_payload)
      subject
    end
  end

  describe '#track' do
    subject { loader.track(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/track.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"project_identifier|343960393583",
      :message_id=>"AMPLITUDE|894128063",
      :timestamp=>Time.parse('2022-01-11 10:02:56.808000000 +0000'),
      :context=>{
        "ip"=>nil,
        "library"=>{
          "name"=>"AmplitudeIntegration",
          "version"=>"0.1.0"
        }},
      :properties=>{
        "amplitude_user_id"=>343960393583,
        "revenue"=>"99.00",
        "$revenue"=>99.0,
        "$quantity"=>1,
        "$price"=>99.0
      },
      :event=>nil
    } }

    it 'sends track' do
      expect(processor).to receive(:track).with(expected_payload)
      subject
    end
  end
end
