# run the plan for the environment of choice 
sqlmesh plan retail_dev --start today

# then execute (add option to ignore any cron schedules & enforce run)
sqlmesh run retail_dev --ignore-cron