require 'spec_helper'

describe AmplitudeProcessor::Loader do
  let(:loader) { described_class.new(sender, 'project_identifier', 'aws_s3_bucket', 'aws_access_key_id', 'aws_secret_access_key', **more_args) }
  let(:sender) { AmplitudeProcessor::Senders::Null.new }

  let(:more_args) { {} }

  before do
    allow(Aws::S3::Client).to receive(:new)
  end

  context 'when user has properties' do
    subject { loader.identify(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/with_user_properties.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"AMPLITUDE|project_identifier|343954024786",
      :message_id=>"AMPLITUDE|508e4a9e-72c5-11ec-b741-02833e729e33",
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
      expect(sender).to receive(:identify).with(expected_payload)
      subject
    end
  end

  context 'when page event' do
    subject { loader.page(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/page.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"AMPLITUDE|project_identifier|346630956846",
      :message_id=>"AMPLITUDE|94fd9096-72c5-11ec-8424-0a0503ad65cb",
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
      :name=>'Loaded a Page'
    } }

    it 'sends page' do
      expect(sender).to receive(:page).with(expected_payload)
      subject
    end
  end

  context 'when track event' do
    subject { loader.track(hash) }

    let(:hash) { JSON.parse(File.read('spec/fixtures/track.json')) }
    let(:expected_payload) { {
      :anonymous_id=>"AMPLITUDE|project_identifier|343960393583",
      :message_id=>"AMPLITUDE|a845ed08-72c5-11ec-8362-06e339fa9735",
      :timestamp=>Time.parse('2022-01-11 10:02:56.808000000 +0000'),
      :context=>{
        "ip"=>nil,
        "library"=>{
          "name"=>"AmplitudeIntegration",
          "version"=>"0.1.0"
        }},
      :properties=>{
        "amplitude_user_id"=>343960393583,
        "revenue"=>expected_revenue,
        "$revenue"=>98.0,
        "$quantity"=>1,
        "$price"=>99.0
      },
      :event=>'Invoice Paid',
      :user_id=> '3762'
    } }
    let(:expected_revenue) { "99.00" }

    it 'sends track' do
      expect(sender).to receive(:track).with(expected_payload)
      subject
    end

    context 'when using custom revenue field' do
      let(:more_args) { { revenue_field: '$revenue' } }
      let(:expected_revenue) { 98.0 }

      it 'is used instead for revenue' do
        expect(sender).to receive(:track).with(expected_payload)
        subject
      end
    end
  end
end
