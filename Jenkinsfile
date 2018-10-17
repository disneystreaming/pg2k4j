fileLoader.withGit(
        'https://github.bamtech.co/bamnet-search/search-jenkins',
        'master',
        'github-user-search'
) {
    utils = fileLoader.load('utils.groovy')
    source = fileLoader.load('source.groovy')
}

node("docker") {
    deleteDir()
    checkout scm

    def dockerTag = 'p13n-pg2k4j'
    def slackChannel = "#p13n-builds"
    def slackTokenId = "p13n-slack"

    def includedFiles = [
            "Dockerfile",
            "Jenkinsfile",
            "src/.*"
    ]

    def skip = utils.jobShouldBeSkipped(includedFiles, [])

    def repoData = env.JOB_NAME.split('/')
    def orgName = repoData[0]

    def onMasterBranch = "${env.BRANCH_NAME}" == "master"
    def inOrg = "${orgName}" == "personalization"
    def shouldRelease = !skip && onMasterBranch && inOrg
    def version = null
    lock("pg2k4j -- ${env.BRANCH_NAME}") {
        stage('Setup') {
            utils.kickoffMessage(slackChannel, slackTokenId)
            utils.ecrLogin()
        }

        stage('Build') {
            sh "docker build -t ${dockerTag} ."
        }

        stage('Test') {
            sh "docker run -w /src ${dockerTag} clean test"
        }

        stage('Bump') {
            if (shouldRelease) {

                version = source.bumpVersionFile("./.version")
                source.pushBumpCommit(version, "bot.p13n")
            }
        }

        ## push bump commit

        def nextVersionTag = (nextVersion.startsWith("v")) ? "${nextVersion}" : "v${nextVersion}"
        origin = "https://${env.GIT_USERNAME}:${env.GIT_PASSWORD}@github.bamtech.co/${orgName}/${repoName}"
        sh "git config user.name Jenkins"
        sh "git config user.email ContentDiscovery@bamtechmedia.com"

        // bump
        sh "git commit -m 'Bumping to ${nextVersionTag}'"
        sh "git push ${origin} ${env.BRANCH_NAME}"

        //tag
        sh "git tag -a ${nextVersionTag} -m Release\\ ${nextVersionTag}"
        sh "git push ${origin} ${nextVersionTag}"

        ## bump version file

          def currentVersion = sh(returnStdout: true, script: "cat ${filepath}").trim()
          def nextVersion = getNextVersion(currentVersion)
          def versionRegex = "'s/${currentVersion}/${nextVersion}/'"
          bumpFile(filepath, versionRegex, nextVersion)
          return nextVersion


          ###
          def getNextVersion(currentVersion) {
              return sh(returnStdout: true, script: "echo '${currentVersion}' | awk -F. '{\$NF = \$NF + 1;} 1' | sed 's/ /./g'").trim()
          }


        stage('DeployToArtifactory') {
            if (shouldRelease) {
                withCredentials([
                        [
                                $class       : 'FileBinding',
                                credentialsId: 'artifactory-maven-settings',
                                variable     : 'M2'
                        ]
                ]) {
                    sh "docker run -v ${M2}:/root/.m2/settings.xml -w /src ${dockerTag} -Drevision=${version} clean deploy"
                }
            }
        }
    }
}
