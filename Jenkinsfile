fileLoader.withGit(
  'https://github.bamtech.co/bamnet-search/search-jenkins',
  'b5752e5c59201aa2b481adcf48687e4c2aa560b7',
  'github-user-search'
) {
  source = fileLoader.load('source.groovy')
  dockerImage = fileLoader.load('dockerImage.groovy')
  janus = fileLoader.load('janus.groovy')
  utils = fileLoader.load('utils.groovy')
}

node("docker"){
  deleteDir()
  checkout scm

  def ecrName = 'kirby-pg2k4j'
  def defaultEnvs = "dev qa"
  def noVersion = ""
  def defaultForceBuild = false

  def slackChannel = "#p13n-builds"
  def slackTokenId = "p13n-slack"

  properties([
    [
      $class: 'ParametersDefinitionProperty', parameterDefinitions: [
        [
            $class: 'StringParameterDefinition',
            defaultValue: "${defaultEnvs}",
            description: 'Space-seperated list of environments. Note that beta will be deployed whenever prod is deployed. E.g. `dev qa prod`',
            name: 'ENVS'
        ],
        [
            $class: 'StringParameterDefinition',
            defaultValue: "${noVersion}",
            description: 'The tag of the docker image to label with the above env tags for deployment. If omitted and on the master branch, the newly built image will recieve both a version tag and the above env tags. If ommitted and not on the master branch, the newly built image will not receive any env tags.',
            name: 'DOCKER_TAG'
        ],
        [
            $class: 'BooleanParameterDefinition',
            defaultValue: defaultForceBuild,
            description: 'If checked, Jenkins will not abort jobs due to the author of the previous commit or because only certain files changed.',
            name: 'FORCE_BUILD'
        ]
      ]
    ]
  ])

  def envsAsString = defaultEnvs
  def forceBuild = defaultForceBuild
  def version = noVersion

  try { envsAsString = "${ENVS}".trim() } catch(e) { echo "using default ENVS" }
  try { forceBuild = "${FORCE_BUILD}".equalsIgnoreCase("true") } catch(e) { echo "using default FORCE_BUILD" }
  try { version = "${DOCKER_TAG}".trim() } catch(e) { echo "using default DOCKER_TAG" }

  def envs = "${envsAsString}".toLowerCase().split()

  def priorVersion = ""
  def priorVersionTag = ""
  def buildImage = null

  def includedFiles = [
    "Dockerfile",
    "src/.*"
  ]

  def neverBuild = !forceBuild && utils.jobShouldBeSkipped(includedFiles, [])
  def shouldBuild =  (!neverBuild && version == "${noVersion}")

  def repoData = env.JOB_NAME.split('/')
  def orgName = repoData[0]
  def repoName = repoData[1]

  def onMasterBranch = "${env.BRANCH_NAME}" == "master"
  def inOrg = "${orgName}" == "personalization"
  def shouldBump = shouldBuild && onMasterBranch && inOrg

  lock("kirby-pg2k4j -- ${env.BRANCH_NAME}") {
    stage('Setup') {
      utils.kickoffMessage(slackChannel, slackTokenId)
      utils.ecrLogin()
    }

    stage('Bump') {
      if (shouldBump) {
        version = source.bumpVersionFile()
      }
    }

    stage('Build and Test') {
      if (shouldBuild) {
        buildImage = dockerImage.build(ecrName, ["--build-arg", "project_version=${version}"], 'p13n')
      }
    }

    stage('Push') {
      if (shouldBump) {
        source.pushBumpCommit(version, "bot.p13n")
      }
      if (shouldBuild) {
        dockerImage.push(buildImage, version, slackChannel, slackTokenId)
      }
    }

    stage('Pull') {
      if (version != "${noVersion}" && buildImage == null) {
        buildImage = dockerImage.pull(ecrName, version, 'p13n')
      }
    }

    stage('DEV Deploy') {
      if (version != "${noVersion}" && envs.contains('dev')) {
        janus.tagAndDeployOneEnvInParallel(buildImage, 'dev', ['kirby-pg2k4j'], "p13n-config", slackChannel, slackTokenId)
      }
    }

    stage('QA Deploy') {

      if (version != "${noVersion}" && envs.contains('qa')) {
        janus.tagAndDeployOneEnvInParallel(buildImage, 'qa', ['kirby-pg2k4j'], "p13n-config", slackChannel, slackTokenId)
      }
    }

    stage('PROD Deploy') {
      if (version != "${noVersion}" && envs.contains('prod')) {
        janus.tagAndDeployOneEnvInParallel(buildImage, 'prod', ['kirby-pg2k4j'], "p13n-config", slackChannel, slackTokenId)
      }
    }
  }
}
