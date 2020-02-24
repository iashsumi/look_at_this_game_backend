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
      # viewsを時間の降順でソート
      views_sorted = item['views'].sort.reverse
      # 最新の時間
      latest = views_sorted.first.first
      days = last_10_days(latest)
      # 日毎の再生数
      views_each_day = {}
      days.each do | i |
        temp = item['views'].select {|k,v| Date.parse(k).strftime('%Y/%m/%d') ==  i}
        next if temp.blank?
        views_each_day.merge!([temp.sort.first].to_h)
      end
      {
        channel_id: item['channel_id'],
        channel_name: item['channel_name'],
        channel_thumbnail_url: item['channel_thumbnail_url'],
        description: item['description'],
        video_id: item['video_id'],
        sort_key: item['sort_key'],
        site_id: item['site_id'].to_i,
        latest_view: views_sorted.first.last.to_i,
        views_each_day: views_each_day,
        updated_at: item['updated_at'],
        thumbnail_url: item['thumbnail_url'],
        link: item['link'],
        title: item['title'],
        published_at: item['published_at']
      }
    end
  end

  # 過去10日間の配列
  def last_10_days(latest)
    base_date = Date.parse(latest)
    [
      base_date.strftime('%Y/%m/%d'),
      (base_date-1).strftime('%Y/%m/%d'),
      (base_date-2).strftime('%Y/%m/%d'),
      (base_date-3).strftime('%Y/%m/%d'),
      (base_date-4).strftime('%Y/%m/%d'),
      (base_date-5).strftime('%Y/%m/%d'),
      (base_date-6).strftime('%Y/%m/%d'),
      (base_date-7).strftime('%Y/%m/%d'),
      (base_date-8).strftime('%Y/%m/%d'),
      (base_date-9).strftime('%Y/%m/%d'),
      (base_date-10).strftime('%Y/%m/%d')
    ]
  end
