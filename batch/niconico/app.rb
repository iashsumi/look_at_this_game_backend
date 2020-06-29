require 'active_support/all'
require "aws-sdk"
require 'date'
require "json"
require 'net/http'
require 'rss'
require 'uri'
require 'securerandom'

NICONICO_SITE_ID = 1
TABLE_1 = ENV['TABLE_1']
TABLE_2 = ENV['TABLE_2']

def lambda_handler(event:, context:)
  uuid = SecureRandom.uuid
  puts "START-#{uuid}"
  return if health_check(event)

  client = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  target = commentators(client)
  return if target.blank?

  target.to_h[:items].each do | commentator |
    channel_id = commentator["user_id"]
    endpoint = "https://www.nicovideo.jp/user/#{channel_id}/video?rss=2.0"
    response = Net::HTTP.get(URI.parse(endpoint))
    updated_at = DateTime.now.strftime('%Y/%m/%d %H:%M:%S')
    return if response.blank?

    Hash.from_xml(response)['rss']['channel']['item'].each do | item | 
      title = item['title']&.first
      api_result = search_video_contents(title)
      next if api_result.blank? || api_result['data'].blank?

      published_at = DateTime.parse(api_result['data'].first['startTime']).strftime('%Y/%m/%d %H:%M:%S')
      video_id = api_result['data'].first['contentId']
      rss_info = find_rss_info(client, { published_at: published_at, video_id: video_id})
      if rss_info.present?
        params = { item: item, api_result: api_result, rss_info: rss_info, published_at: published_at, video_id: video_id, updated_at: updated_at }
        update_rss_info(client, params)
      else
        params = { commentator: commentator, item: item, api_result: api_result, published_at: published_at, video_id: video_id, updated_at: updated_at }
        create_rss_info(client, params)
      end
    end
  end
  puts "END-#{uuid}"
end

private

  def health_check(event)
    value = event['queryStringParameters']['health_check'] if event['queryStringParameters'].present?
    if value == 'true'
      puts 'health check!!' 
      return true
    else
      puts 'execute!!'
      return false 
    end
  end

  def commentators(client)
    client.scan({
      expression_attribute_names: {
        "#UI" => "user_id", 
        "#NAME" => "name", 
        "#TU" => 'thumbnail_url'
      }, 
      expression_attribute_values: {
        ":site_id" => NICONICO_SITE_ID, 
      }, 
      filter_expression: "site_id = :site_id", 
      projection_expression: "#UI, #NAME, #TU", 
      table_name: TABLE_1
    })
  end

  # call niconico contents api
  def search_video_contents(title)
    endpoint = "https://api.search.nicovideo.jp/api/v2/video/contents/search"
    url = URI.parse(endpoint)
    params = Hash.new
    params.store('q', title)
    params.store('targets', 'title')
    params.store('fields', 'description,startTime,thumbnailUrl,userId,contentId,title,viewCounter')
    params.store('_sort', '-viewCounter')
    params.store('_context', 'apiguide')
    url.query = URI.encode_www_form(params)
    req = Net::HTTP::Get.new(url)
    try = 0
    begin
      try += 1
      res = Net::HTTP.start(url.host, url.port, :use_ssl => true ) {|http|
        http.request(req)
      }
      raise 'unknown error' if res.code != '200' && res.code != '400'
    rescue
      sleep 2
      retry if try < 3
      return nil
    end
    JSON.parse(res.body)
  end

  def find_rss_info(client, params)
    sort_key = "#{params[:published_at]}-#{params[:video_id]}"
    client.get_item({
      key: {
        dummy_key: 'dummy_key', 
        sort_key: sort_key
      }, 
      table_name: TABLE_2
    })
  end

  def update_rss_info(client, params)
    views = params[:rss_info]['item']['views']
    updated_at = params[:updated_at]
    views[updated_at] = params[:api_result]['data'].first['viewCounter']
    sort_key = "#{params[:published_at]}-#{params[:video_id]}"
    client.update_item({
      expression_attribute_names: {
        "#VIEWS" => "views", 
        "#UPDATED_AT" => "updated_at"
      },
      expression_attribute_values: {
        ":views" => views, 
        ":updated_at" => updated_at 
      },
      key: {
        dummy_key: 'dummy_key', 
        sort_key: sort_key
      }, 
      table_name: TABLE_2, 
      update_expression: "SET #VIEWS = :views, #UPDATED_AT = :updated_at"
    })
  end

  def create_rss_info(client, params)
    sort_key = "#{params[:published_at]}-#{params[:video_id]}"
    client.put_item({
      item: {
        dummy_key: 'dummy_key', 
        sort_key: sort_key,
        published_at: params[:published_at],
        video_id: params[:video_id],
        site_id: NICONICO_SITE_ID,
        channel_id: params[:commentator]['user_id'],
        channel_name: params[:commentator]['name'],
        channel_thumbnail_url: params[:commentator]['thumbnail_url'],
        title: params[:api_result]['data'].first['title'],
        link: params[:item]['link'],
        description: params[:api_result]['data'].first['description'],
        thumbnail_url: params[:api_result]['data'].first['thumbnailUrl'],
        views: { params[:updated_at] => params[:api_result]['data'].first['viewCounter'], },
        updated_at: params[:updated_at]
      },
      table_name: TABLE_2
    })
  end
