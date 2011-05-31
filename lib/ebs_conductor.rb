#  Copyright 2011 Ryan J. Geyer
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require 'skeme'
require 'fog'
require 'rest_connection'
require 'yaml'

module EbsConductor
  class EbsConductor
    @@skeme = nil
    @@fog_aws_computes = {}
    @@have_rs = false
    @@logger = nil
    @@default_timeout = 5*60
    @@timeout_backoff = [2,5,10,15]

    def initialize(aws_access_key_id, aws_secret_access_key, options={:rs_email => nil, :rs_pass => nil, :rs_acct_num => nil})
      if options[:logger]
        @@logger = options[:logger]
      else
        @@logger = Logger.new(STDOUT)
      end

      @@skeme = Skeme::Skeme.new({
        :aws_access_key_id => aws_access_key_id,
        :aws_secret_access_key => aws_secret_access_key,
        :rs_email => options[:rs_email],
        :rs_pass => options[:rs_pass],
        :rs_acct_num => options[:rs_acct_num]
      })

      if options[:rs_email] && options[:rs_pass] && options[:rs_acct_num]
        ::RightScale::Api::BaseExtend.class_eval <<-EOF
        @@connection ||= RestConnection::Connection.new
          @@connection.settings = {
            :user => "#{options[:rs_email]}",
            :pass => "#{options[:rs_pass]}",
            :api_url => "https://my.rightscale.com/api/acct/#{options[:rs_acct_num]}",
            :common_headers => {
              "X_API_VERSION" => "1.0"
            }
          }
        EOF
        ::RightScale::Api::Base.class_eval <<-EOF
        @@connection ||= RestConnection::Connection.new
          @@connection.settings = {
            :user => "#{options[:rs_email]}",
            :pass => "#{options[:rs_pass]}",
            :api_url => "https://my.rightscale.com/api/acct/#{options[:rs_acct_num]}",
            :common_headers => {
              "X_API_VERSION" => "1.0"
            }
          }
        EOF

        @@have_rs = true
      end

      fog_aws_compute = Fog::Compute.new({:aws_access_key_id => aws_access_key_id, :aws_secret_access_key => aws_secret_access_key, :provider => 'AWS'})
      fog_aws_compute.describe_regions.body['regionInfo'].each do |region|
        @@fog_aws_computes.store(region['regionName'],
          Fog::Compute.new({
            :aws_access_key_id => aws_access_key_id,
            :aws_secret_access_key => aws_secret_access_key,
            :provider => 'AWS',
            :host => region['regionEndpoint']
          })
        )
      end
    end

    def find_instance_by_id(instance_id)
      @@fog_aws_computes.each do |key,val|
        srv = val.servers.get(instance_id)
        if srv
          return { :server => srv, :region => key }
        end
      end
      nil
    end

    def find_volume_by_id(volume_id, options={:region => nil})
      if options[:region]
        return {:volume => @@fog_aws_computes[options[:region]].volumes.get(volume_id), :region => options[:region]}
      else
        @@fog_aws_computes.each do |key,val|
          vol = val.volumes.get(volume_id)
          if vol
            return {:volume => vol, :region => key}
          end
        end
      end

      nil
    end

    def find_volumes_by_lineage(lineage)
      vols = {}
      @@fog_aws_computes.each do |key,val|
        volz = val.volumes
        vols[key] = volz.all('tag-key' => lineage_tag(lineage))
      end

      vols
    end

    def find_snapshots_in_lineage(lineage, options={:region => nil})
      snaps = {}
      if options[:region] != nil
        snapshots_in_lineage = @@fog_aws_computes[options[:region]].snapshots.all('tag-key' => lineage_tag(lineage))
        if snapshots_in_lineage
          snaps[options[:region]] = snapshots_in_lineage
        end
      else
        @@fog_aws_computes.each do |region,compute|
          snapshots_in_lineage = compute.snapshots.all('tag-key' => lineage_tag(lineage))
          if snapshots_in_lineage
            snaps[region] = snapshots_in_lineage
          end
        end
      end

      snaps
    end

    def attach_from_lineage(instance_id, lineage, size_in_gb, device, options={:timeout => @@default_timeout, :snapshot_id => nil, :tags => nil})
      # Explore our options.
      # if snapshot_id is supplied and is a snapshot id, try to create & attach it
      # Find the newest available snapshot in the lineage and attach if available

      lineage_tag_key = lineage_tag(lineage)

      server_hash = find_instance_by_id(instance_id)
      if !server_hash
        raise "Instance #{instance_id} was not found!"
      end
      server = server_hash[:server]
      region = server_hash[:region]

      snapshot_id = options[:snapshot_id]
      if !options[:snapshot_id]
        snapshots_in_lineage = @@fog_aws_computes[region].snapshots.select { |snap| snap.tags.keys.include? lineage_tag_key }
        if snapshots_in_lineage && snapshots_in_lineage.count
          latest_snap = snapshots_in_lineage.sort! { |a,b| b.created_at <=> a.created_at }.first
          if latest_snap
            snapshot_id = latest_snap.id
          end
        end
      end

      new_vol = server.volumes.new({:snapshot_id => snapshot_id, :size => size_in_gb, :device => device})
      new_vol.save()

      timeout_message = "Timed out waiting for EBS volume to be created and attached to (#{server.id}).  Elapsed time was #{options[:timeout]} seconds"
      block_until_timeout(timeout_message, options[:timeout]) {
        keep_baking = false
        # Check things out on AWS
        check_vol = find_volume_by_id(new_vol.id, region)[:volume]
        if check_vol && check_vol.state != "deleting"
          keep_baking = true unless check_vol.state == "in-use" && check_vol.server_id == server.id
        else
          raise "The EBS Volume #{new_vol.id} does not exist, or is being deleted"
        end

        # Check things out in RS if we've got RS credentials
        if @@have_rs
          vol = Ec2EbsVolume.find(:first) { |vol| vol.aws_id == new_vol.id }
          keep_baking = (vol == nil)
        end

        keep_baking
      }

      @@skeme.set_tag({:ec2_ebs_volume_id => new_vol.id, :tag => lineage_tag(lineage)})
      if options[:tags] && options[:tags].kind_of?(Array)
        options[:tags].each do |tag|
          @@skeme.set_tag({:ec2_ebs_volume_id => new_vol.id, :tag => tag})
        end
      end

      new_vol.id
    end

    # Snapshot history to keep is unique to each region.  If a volume_id is specified, the new snapshot will be tagged
    # with the supplied lineage, regardless of the volume's current lineage.  This effectively allows you to override the lineage of a volume
    #
    def snapshot_lineage(lineage, options={:timeout => @@default_timeout, :tags => nil, :volume_id => nil, :history_to_keep => nil})
      vol_hash = {}
      tag_hash ={}
      if options[:volume_id]
        vol_by_id = find_volume_by_id(options[:volume_id])
        vol_hash[vol_by_id[:region]] = [vol_by_id[:volume]]
      else
        vol_hash = find_volumes_by_lineage(lineage)
      end

      vol_hash.each do |region,vols|
        # TODO: warn about multiples in a region?
        vols.each do |vol|
          description = "Created by EBS Conductor for the (#{lineage}) lineage while the volume was #{vol.server_id ? "attached to #{vol.server_id}" : "detatched"}"

          excon_resp = @@fog_aws_computes[region].create_snapshot(vol.id, description)
          snapshot_id = excon_resp.body['snapshotId']

          tags = options[:tags] || []
          tags << lineage_tag(lineage)
          tag_hash[snapshot_id] = {:snapshot_tags => tags, :volume_tags => vol.tags.keys}
        end
      end

      timeout_message = "Timed out waiting for EBS snapshots to start from volumes [#{vol_hash.collect{|key,val| val}}].  Elapsed time was #{options[:timeout]}"
      block_until_timeout(timeout_message, options[:timeout]) {
        keep_baking = false

        if @@have_rs
          snaps = Ec2EbsSnapshot.find(:all) { |snap| tag_hash.keys.include? snap.aws_id }
          keep_baking = (snaps.count != tag_hash.keys.count)
        end

        keep_baking
      }

      # TODO: check for existing lineage which may be getting overwritten. Maybe warn, maybe just tag accordingly?
      tag_hash.each do |key,val|
        val[:snapshot_tags].each do |tag|
          @@skeme.set_tag(:ec2_ebs_snapshot_id => key, :tag => tag)
        end
      end

      if options[:history_to_keep] && options[:history_to_keep].kind_of?(Integer)
        @@fog_aws_computes.keys.each do |region|
          snaps = find_snapshots_in_lineage(lineage, {:region => region})
          snaps.each do |key,val|
            val.sort! { |a,b| a.created_at <=> b.created_at }
            delete_count = (val.count - options[:history_to_keep])-1
            (0..delete_count).each do |idx|
              vol = val[idx]
              vol.destroy
              @@logger.info("Deleted snapshot #{vol.id}")
            end unless delete_count <= -1
          end
        end
      end
    end

    private

    def block_until_timeout(timeout_message, timeout, &block)
      begin
        idx=0
        Timeout::timeout(timeout) do
          while true
            if yield
              sleep @@timeout_backoff[idx] || @@timeout_backoff.last
              idx += 1
            else
              break
            end
          end
        end
      rescue Timeout::Error
        raise timeout_message
      end
    end

    def lineage_tag(lineage)
      "ebs_conductor:lineage=#{lineage}"
    end

  end
end