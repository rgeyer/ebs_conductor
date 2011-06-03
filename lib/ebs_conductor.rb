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

module Rgeyer
  module Gem
    class EbsConductor
      @@skeme = nil
      @@fog_aws_computes = {}
      @@have_rs = false
      @@logger = nil
      @@default_timeout = 5*60
      @@timeout_backoff = [2,5,10,15]

      # Instantiates a new EbsConductor
      #
      # Amazon Web Services (AWS) credentials are required.  RightScale credentials are optional, if provided all
      # objects (volumes & snapshots) will be tagged in both EC2 and RightScale
      #
      # == Parameters
      # * *aws_access_key_id* : The access key ID for the AWS API.
      # * *aws_secret_access_key* : The secret access key (password) for the AWS API.
      #
      # === Options
      # * :rs_email => 'foo@bar.baz' : The email address of a RightScale user with permissions to tag volumes & snapshots
      # * :rs_pass => 'supersecret' : The password of a RightScale user with permissions to tag volumes & snapshots
      # * :rs_acct_num => 123456 : Your RightScale account number
      # * :logger => A logger object
      #
      # == Examples
      # Create an EBS conductor which will only tag objects in EC2
      #     Rgeyer::Gem::EbsConductor.new('...','...')
      #
      # Create an EBS conductor which will tag objects in EC2 and RightScale
      #     Rgeyer::Gem::EbsConductor.new('...','...',{:rs_email => '...', :rs_pass => '...', :rs_acct_num => 123456})
      #
      # Create an EBS conductor which will tag objects in EC2 and RightScale, and log to Chef::Log
      #     Rgeyer::Gem::EbsConductor.new('...','...',{:rs_email => '...', :rs_pass => '...', :rs_acct_num => 123456, :logger => Chef::Log })
      #
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

      # Attaches a volume from the specified lineage to the specified EC2 instance.
      #
      # The source of the new volume is as follows (in order of preference)
      # * A new volume from the :snapshot_id option (if supplied)
      # * The newest snapshot created by ebs_conductor for the specified lineage, provided it is in the same region as the server
      # * A new blank volume
      #
      # == Parameters
      # * *instance_id* : The AWS id of the server instance which should have the new volume attached.  I.E. i-[0-9a-z]{8}
      # * *lineage* : The name of the lineage to attach.  *NOTE*: The lineage must be unique to an AWS account to avoid problems!
      # * *size_in_gb* : The size of the new volume, measured in gigabytes (GB)
      # * *device* : A valid device that the new volume will be attached to.  For Windows this is xvdf - xvdp, and for Linux it is /dev/sdb - /dev/sdp
      #
      # === Options
      # * :timeout => @@default_timeout : The timeout in seconds before EBS conductor should stop waiting for a volume to be created and attached.  The default is 5 minutes
      # * :snapshot_id => '...' : The AWS ID of a snapshot to create the new volume from.  I.E. snap-[0-9a-z]{8}
      # * :tags => [] : An array of strings which will be applied as additional tags to the new volume.  I.E. ["foo:bar=baz", "database:name=sweet"]
      #
      # == Examples
      # All examples assume that a new EBS conductor has been created and is assigned to *ebs_conductor*
      #     ebs_conductor = Rgeyer::Gem::EbsConductor.new('...','...')
      #
      # Attach a new 1GB blank volume in the lineage "foobar" to a linux box at /dev/sdb1
      #     ebs_conductor.attach_from_lineage('i-abcd1234', 'foobar', 1, '/dev/sdb1')
      #
      # Attach a specific snapshot to a 1GB volume in the lineage "foobar" to a linux box at /devb/sdb1
      #     ebs_conductor.attach_from_lineage('i-abcd1234', 'foobar', 1, '/dev/sdb1' {:snapshot_id => 'snap-abcd1234'})
      #
      def attach_from_lineage(instance_id, lineage, size_in_gb, device, options={:timeout => @@default_timeout, :snapshot_id => nil, :tags => nil})

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
          server = @@fog_aws_computes[region].servers.get(instance_id)
          # Check things out on AWS
          check_vol = server.block_device_mapping.select { |dev| dev['volumeId'] == new_vol.id }.first
          keep_baking = true if !check_vol || check_vol['status'] != "attached"

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

      # Creates a new snapshot of the specified lineage.  Optionally purges previous snapshots in the lineage based on the :history_to_keep option
      #
      # == Parameters
      # * *lineage*: The name of the lineage to snapshot.  *NOTE*: The lineage must be unique to an AWS account to avoid problems!
      #
      # === Options
      # * :timeout => @@default_timeout : The timeout in seconds before EBS conductor should stop waiting for a volume to be created and attached.  The default is 5 minutes
      # * :volume_id => '...' : The AWS ID of a volume to create the snapshot of.  I.E. vol-[0-9a-z]{8}
      # * :history_to_keep => 7 : If supplied only :history_to_keep snapshots will be kept for the lineage.  If there are more than :history_to_keep snapshots for the lineage, the oldest ones are deleted
      # * :tags => [] : An array of strings which will be applied as additional tags to the new snapshot.  I.E. ["foo:bar=baz", "database:name=sweet"]
      #
      # == Examples
      # All examples assume that a new EBS conductor has been created and is assigned to *ebs_conductor*
      #     ebs_conductor = Rgeyer::Gem::EbsConductor.new('...','...')
      #
      # Snapshot the lineage "foobar", do not purge any old snapshots in the lineage
      #     ebs_conductor.snapshot_lineage('foobar')
      #
      # Snapshot the lineage "foobar", and purge old snapshots so that only 7 remain
      #     ebs_conductor.snapshot_lineage('foobar', {:history_to_keep => 7})
      #
      # Snapshot the lineage "foobar" from the specified volume_id.  This is useful if you're trying to start a lineage from a "naked" instance, or if you are trying to create a new lineage from an existing one
      #     ebs_conductor.snapshot_lineage('foobar', {:history_to_keep => 7, :volume_id => 'vol-abcd1234'})
      #
      def snapshot_lineage(lineage, options={:timeout => @@default_timeout, :volume_id => nil, :history_to_keep => nil, :tags => nil})
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
            if ["available", "in-use"].include? vol.status
              description = "Created by EBS Conductor for the (#{lineage}) lineage while the volume was #{vol.server_id ? "attached to #{vol.server_id}" : "detatched"}"

              excon_resp = @@fog_aws_computes[region].create_snapshot(vol.id, description)
              snapshot_id = excon_resp.body['snapshotId']

              tags = options[:tags] || []
              tags << lineage_tag(lineage)
              tag_hash[snapshot_id] = {:snapshot_tags => tags, :volume_tags => vol.tags.keys}
            else
              @@logger.warn("Volume (#{vol.id}) had a status of (#{vol.status}).  A snapshot could not be created..")
            end
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
end