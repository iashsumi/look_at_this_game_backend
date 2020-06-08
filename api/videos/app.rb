require 'active_support/all'
require "aws-sdk"
require 'date'
require "json"
require 'net/http'

TABLE_2 = ENV['TABLE_2']

def lambda_handler(event:, context:)
  client = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  paging_key = JSON.parse(event['queryStringParameters']['paging_key']) if event['queryStringParameters'].present?
  target = videos(client, paging_key)
  return { statusCode: 400 } if target.blank?
  {
    statusCode: 200,
    body: {
      count: target.count,
      scanned_count: target.scanned_count,
      paging_key: target.last_evaluated_key,
      items: build_response(target.items)
    }.to_json
  }
end

private

  def videos(client, paging_key)
    params = { 
      key_condition_expression: 'dummy_key = :v1',
      expression_attribute_values: {
        ':v1': 'dummy_key'
      },
      scan_index_forward: false, 
      limit: 50, 
      table_name: TABLE_2 
    }
    params[:exclusive_start_key] = paging_key if paging_key.present?
    client.query(params)
  end

  def build_response(items)
    items.map do | item |
      {
        channel_id: item['channel_id'],
        channel_name: item['channel_name'],
        channel_thumbnail_url: item['channel_thumbnail_url'],
        description: item['description'],
        video_id: item['video_id'],
        sort_key: item['sort_key'],
        site_id: item['site_id'].to_i,
        views: item['views'],
        updated_at: item['updated_at'],
        thumbnail_url: item['thumbnail_url'],
        link: item['link'],
        title: item['title'],
        published_at: item['published_at']
      }
    end
  end
