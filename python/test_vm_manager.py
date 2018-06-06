import ec2_vm

vm_manager = ec2_vm.Ec2VMManager("Manager to handle VMs in the cloud")

vm_manager.list_all_vm_instances()
vm_manager.list_vm_instances_with_tag("cirrus")

vm_manager.start_vm()
