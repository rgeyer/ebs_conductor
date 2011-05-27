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

module EbsConductor
  class EbsConductor
    @@skeme = nil
    @@fog_aws_compute = nil
    @@logger = nil
    @@device_list_ary = []

    def initialize(aws_access_key_id, aws_secret_access_key, options={:rs_email => nil, :rs_pass => nil, :rs_acct_num => nil})
      if options[:logger]
        @@logger = options[:logger]
      else
        @@logger = Logger.new(STDOUT)
      end

      # This is for windows, but it actually seems as though that's not necessary
#      @@device_list_ary = []
#      ("b".."p").each do |letter|
#        device_list_ary << "xvd#{letter}"
#      end

      @@device_list_ary = []
      ("a".."p").each do |letter|
        (1..15).each do |number|
          device_list_ary << "/dev/sd#{letter}#{number}"
        end
      end

      @@skeme = Skeme::Skeme.new({
        :aws_access_key_id => aws_access_key_id,
        :aws_secret_access_key => aws_secret_access_key,
        :rs_email => rs_email,
        :rs_pass => rs_pass,
        :rs_acct_num => rs_acct_num
      })

      @@fog_aws_compute = Fog::Compute.new({:aws_access_key_id => aws_access_key_id, :aws_secret_access_key => aws_secret_access_key, :provider => 'AWS'})
    end

    def find_volume_by_id(volume_id)
      @@fog_aws_compute.volumes.get(volume_id)
    end

    # Creates a new EBS volume and blocks until the volume is available
    # ==Returns
    # Returns the AWS volume id if all went well
    def create(instance_id, lineage, size_in_gb, options={:timeout => 5*60, :snapshot_id => nil, :device => nil})
      # Explore our options.
      # If snapshot_id is supplied and is a volume id, try to attach it
      # if snapshot_id is supplied and is a snapshot id, try to create & attach it
      # Find the newest available volume in the lineage and attach if available
      # Find the newest available snapshot in the lineage and attach if available

      new_vol = @@fog_aws_compute.volumes.new({:snapshot_id => options[:snapshot_id], :size => size_in_gb, :availability_zone => availability_zone}).save()
      @@skeme.set_tag({:ec2_ebs_volume_id => new_vol.id, :tag => "ebs_conductor:lineage=#{lineage}"})

      begin
        Timeout::timeout(options[:timeout]) do
          while true
            check_vol = find_volume_by_id(new_vol.id)
            if check_vol && check_vol.state != "deleting"
              if ["in-use", "available"].include? check_vol.state
                break
              end
              sleep 2
            else
              raise "The EBS Volume #{new_vol.id} does not exist, or is being deleted"
            end
          end
        end
      rescue Timeout::Error
        raise "Timed out waiting for EBS volume to be created.  Elapsed time was #{options[:timeout]} seconds"
      end

      new_vol.id
    end

  end
end