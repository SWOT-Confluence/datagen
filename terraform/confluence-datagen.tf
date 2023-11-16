# Job Definition
resource "aws_batch_job_definition" "generate_batch_jd_datagen" {
  name                  = "${var.prefix}-datagen"
  type                  = "container"
  container_properties  = <<CONTAINER_PROPERTIES
  {
    "image": "${local.account_id}.dkr.ecr.us-west-2.amazonaws.com/datagen",
    "executionRoleArn": "${data.aws_iam_role.exe_role.arn}",
    "jobRoleArn": "${data.aws_iam_role.job_role.arn}",
    "fargatePlatformConfiguration": { "platformVersion": "LATEST" },
    "logConfiguration": {
      "logDriver" : "awslogs",
      "options": {
        "awslogs-group" : "${data.aws_cloudwatch_log_group.cw_log_group.name}"
      }
    },
    "resourceRequirements": [
      {"type": "MEMORY", "value": "4096"},
      {"type": "VCPU", "value": "2"}
    ],
    "mountPoints": [
      {
        "sourceVolume": "input",
        "containerPath": "/data"
      }
    ],
    "volumes": [
      {
        "name": "input",
        "efsVolumeConfiguration": {
          "fileSystemId": "${data.aws_efs_file_system.aws_efs_input.file_system_id}",
          "rootDirectory": "/"
        }
      }
    ]
  }
  CONTAINER_PROPERTIES
  platform_capabilities = ["FARGATE"]
  propagate_tags        = true
   retry_strategy {
    attempts = 3
  }
}
