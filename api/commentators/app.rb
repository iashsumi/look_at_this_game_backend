require 'active_support/all'
require "aws-sdk"
require "json"
require 'net/http'

TABLE_1 = ENV['TABLE_1']

def lambda_handler(event:, context:)
  client = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  target = commentators(client)
  return { statusCode: 400 } if target.blank?
  {
    statusCode: 200,
    body: {
      count: target.count,
      scanned_count: target.scanned_count,
      items: build_response(target.items)
    }.to_json
  }
end

private

  def commentators(client)
    params = { table_name: TABLE_1 }
    client.scan(params)
  end

  def build_response(items)
    items.map do | item |
      {
        user_id: item['user_id'],
        name: item['name'],
        thumbnail_url: item['thumbnail_url'],
        site_id: item['site_id'].to_i
      }
    end
  end
