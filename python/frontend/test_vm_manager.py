import ec2_vm

vm_manager = ec2_vm.Ec2VMManager("Manager to handle VMs in the cloud")

vm_manager.list_all_vm_instances()
vm_manager.list_vm_instances_with_tag("cirrus")

instance_id = vm_manager.start_vm()
vm_manager.wait_until_running(instance_id)
