require 'active_support/all'
require "aws-sdk"
require 'date'
require 'net/http'
require 'rss'

YOUTUBE_SITE_ID = 0
TABLE_1 = ENV['TABLE_1']
TABLE_2 = ENV['TABLE_2']

def lambda_handler(event:, context:)
  client = Aws::DynamoDB::Client.new(region: 'ap-northeast-1')
  
  target = commentators(client)
  return if target.blank?

  target.to_h[:items].each do | commentator |
    channel_id = commentator["user_id"]
    endpoint = "https://www.youtube.com/feeds/videos.xml?channel_id=#{channel_id}"
    response = Net::HTTP.get(URI.parse(endpoint))
    updated_at = DateTime.now.strftime('%Y/%m/%d %H:%M:%S')
    return if response.blank?
    Hash.from_xml(response)['feed']['entry'].each do | item | 
      published_at = DateTime.parse(item['published']).strftime('%Y/%m/%d %H:%M:%S')
      video_id = item['videoId']
      rss_info = find_rss_info(client, { published_at: published_at, video_id: video_id})
      if rss_info.present?
        params = { item: item, rss_info: rss_info, published_at: published_at, video_id: video_id, updated_at: updated_at }
        update_rss_info(client, params)
      else
        params = { commentator: commentator, item: item, published_at: published_at, video_id: video_id, updated_at: updated_at }
        create_rss_info(client, params)
      end
    end
  end
end

private

  def commentators(client)
    client.scan({
      expression_attribute_names: {
        "#UI" => "user_id", 
        "#NAME" => "name", 
        "#TU" => 'thumbnail_url'
      }, 
      expression_attribute_values: {
        ":site_id" => YOUTUBE_SITE_ID, 
      }, 
      filter_expression: "site_id = :site_id", 
      projection_expression: "#UI, #NAME, #TU", 
      table_name: TABLE_1
    })
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
    views[updated_at] = params[:item]['group']['community']['statistics']['views']
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
        site_id: YOUTUBE_SITE_ID,
        channel_id: params[:commentator]['user_id'],
        channel_name: params[:commentator]['name'],
        channel_thumbnail_url: params[:commentator]['thumbnail_url'],
        title: params[:item]['title'],
        link: params[:item]['link']['href'],
        description: params[:item]['group']['description'],
        thumbnail_url: params[:item]['group']['thumbnail']['url'],
        views: { params[:updated_at] => params[:item]['group']['community']['statistics']['views'] },
        updated_at: params[:updated_at]
      },
      table_name: TABLE_2
    })
  end
