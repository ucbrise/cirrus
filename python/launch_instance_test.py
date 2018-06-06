reservation = conn.run_instances( ... )

# NOTE: this isn't ideal, and assumes you're reserving one instance. Use a for loop, ideally.
instance = reservation.instances[0]

# Check up on its status every so often
status = instance.update()
while status == 'pending':
  time.sleep(10)
  status = instance.update()

if status == 'running':
  instance.add_tag("Name","{{INSERT NAME}}")
else:
  print('Instance status: ' + status)
  return None

# Now that the status is running, it's not yet launched. The only way to tell if it's fully up is to try to SSH in.
if status == "running":
  retry = True
  while retry:
    try:
      # SSH into the box here. I personally use fabric
      retry = False
    except:
      time.sleep(10)
