@Library('lib')_

def deployable_branches = ["master"]
maven = 'system_maven'
def config = [:]

def build_container_image    = (config.build_container_image == true) ?: false

def docker_registry_dev      = config.docker_registry_dev ?: globalDefaults.docker_registry_dev()
def docker_registry_prod     = config.docker_registry_prod ?: globalDefaults.docker_registry_prod()
def jdk_version              = 11
def maven_version            = '3.5.4'
def node_label               = 'default'
def wipeout_workspace        = true

def maven_image              = "vito-docker.artifactory.vgt.vito.be/almalinux8.5-spark-py-openeo:3.5.3"


pipeline {
    agent {
        label "default"
    }
    environment {
        BRANCH_NAME = "${env.BRANCH_NAME}"
        BUILD_NUMBER = "${env.BUILD_NUMBER}"
        BUILD_URL = "${env.BUILD_URL}"
        DEFAULT_MAVEN_OPTS = "${default_maven_opts}"
        DOCKER_REGISTRY_DEV = "${docker_registry_dev}"
        DOCKER_REGISTRY_PROD = "${docker_registry_prod}"
        JOB_BASE_NAME = "${env.JOB_BASE_NAME}"
        JOB_NAME = "${env.JOB_NAME}"
        JOB_URL = "${env.JOB_URL}"

        MAVEN_IMAGE = "${maven_image}"
        MAVEN_VERSION = "${maven_version}"
        PACKAGE_NAME = "${package_name}"
        WORKSPACE = "${env.WORKSPACE}"
    }
    parameters {
      booleanParam(name: 'skip_tests', defaultValue: false, description: 'Check this if you want to skip running tests.')
      booleanParam(name: 'skip_sentinelhub_tests', defaultValue: false, description: 'Check this if you want to skip running Sentinel Hub tests.')
    }
    stages {
        stage('Checkout') {
            steps {
                script {
                    git.checkoutDefault(wipeout_workspace)
                    env.GIT_COMMIT = git.getCommit()
                    env.GROUP_ID = java.getGroupId()
                    env.PACKAGE_VERSION = "${java.getRevision()}-${utils.getDate()}-${BUILD_NUMBER}"
                    env.MAIL_ADDRESS = utils.getMailAddress()
                    env.IMAGE_NAME_TAG = "${DOCKER_REGISTRY_DEV}/${PACKAGE_NAME}:${PACKAGE_VERSION}"
                }
            }
        }
        stage('Build and Test') {
            steps {
                script {
                    rel_version = getMavenVersion()
                    build( params.skip_tests, params.skip_sentinelhub_tests)
                    utils.setWorkspacePermissions()
                }
            }
        }

        stage("trigger integrationtests") {
            when {
                expression {
                    ["master", "develop"].contains(env.BRANCH_NAME)
                }
            }
            steps {
                script {
                    if (Jenkins.instance.getItemByFullName("openEO/openeo-integrationtests/master")) {
                        utils.triggerJob("openEO/openeo-integrationtests", ['mail_address': env.MAIL_ADDRESS])
                    } else {
                        utils.triggerJob("openEO/openeo-integrationtests", ['mail_address': env.MAIL_ADDRESS])
                    }
                }
            }

        }


        stage('Input') {
            when {
                expression {
                    deployable_branches.contains(env.BRANCH_NAME)
                }
            }
            steps {
                script {
                    milestone()
                    input "Release build ${rel_version}?"
                    milestone()
                }
            }

        }


        stage('Releasing') {
            when {
                expression {
                    deployable_branches.contains(env.BRANCH_NAME)
                }
            }
            steps {
                script {
                    checkout scm
                    rel_version = getReleaseVersion()
                    withMavenEnv(["JAVA_OPTS=-Xmx1536m -Xms512m", "HADOOP_CONF_DIR=/etc/hadoop/conf/"]) {
                        sh "mvn versions:use-releases -DgenerateBackupPoms=false -DfailIfNotReplaced=true"
                        echo "Removing SNAPSHOT from version for release"
                        sh "mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${rel_version}"
                    }
                    echo "releasing version ${rel_version}"
                    build(skipTests = true)

                    withMavenEnv(["JAVA_OPTS=-Xmx1536m -Xms512m", "HADOOP_CONF_DIR=/etc/hadoop/conf/"]) {
                        withCredentials([[$class: 'UsernamePasswordMultiBinding', credentialsId: 'BobDeBouwer', usernameVariable: 'GIT_USERNAME', passwordVariable: 'GIT_PASSWORD']]) {
                            //sh "git commit -a -m 'Set version v${rel_version} in pom for release'"
                            //sh "git tag -a v${rel_version} -m 'version ${rel_version}'"
                            //sh "git push https://${GIT_USERNAME}:${GIT_PASSWORD}@git.vito.be/scm/biggeo/geotrellistimeseries.git v${rel_version}"
                            sh "git checkout ${env.BRANCH_NAME}"
                            new_version = updateMavenVersion()
                            sh "mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${new_version}"
                            //sh "git commit -a -m 'Raise version in pom to ${new_version}'"
                            //sh "git push https://${GIT_USERNAME}:${GIT_PASSWORD}@git.vito.be/scm/biggeo/geotrellistimeseries.git ${env.BRANCH_NAME}"
                        }
                    }

                    milestone()
                }
            }
        }

    }
    post {
        always {
          script {
            docker.image(maven_image).inside('-u root --entrypoint=""') {
              sh """
                ${utils.setWorkspacePermissions()}
              """
            }
          }
        }
        failure {
          script {
            notification.fail(env.MAIL_ADDRESS)
          }
        }
      }
}
String getMavenVersion() {
    pom = readMavenPom file: 'pom.xml'
    version = pom.version
    if (version == null){
        version = pom.parent.version
    }
    return version
}

String getReleaseVersion() {
    pom = readMavenPom file: 'pom.xml'
    version = pom.version
    if (version == null){
        version = pom.parent.version
    }
    v = version.tokenize('.-') //List['1','0','0','SNAPSHOT']
    v_releasable = v[0] + '.' + v[1] + '.' + v[2] // 1.0.0
    pom.version = v_releasable
    return v_releasable
}

String updateMavenVersion(){
    pom = readMavenPom file: 'pom.xml'
    version = pom.version //1.0-SNAPSHOT
    v = version.tokenize('.-') //List['1','0'] Snapshot has already been removed by getMavenVersion but needs to be readded
    v_major = v[0].toInteger() // 1
    v_minor = v[1].toInteger() // 0
    v_hotfix = v[2].toInteger()

    v_hotfix += 1
    v = (v_major + '.' + v_minor + '.' + v_hotfix).toString()
    v_snapshot = (v_major + '.' + v_minor + '.' + v_hotfix + '-SNAPSHOT').toString()

    return v_snapshot
}

void build(skipTests = false, skipSentinelHubTests = false){
    def publishable_branches = ["master", "develop"]

    List jdkEnv = [ "SPARK_LOCAL_IP=127.0.0.1", "JAVA_HOME=/usr/lib/jvm/java-17-openjdk"]
    def testImage = docker.build("openeo-geotrellis-test-image", "-f ./docker/tests_dockerfile ./docker")
    testImage.inside('-v /var/run/docker.sock:/var/run/docker.sock -v /localdata/M2:/localdata/M2:rw,z -v /home/jenkins/.m2:/home/jenkins/.m2 :rw,z -v /localdata/M2:/localdata/M2:rw -v /etc/hadoop/conf:/etc/hadoop/conf:ro -v /data:/data:ro') {
        withEnv(jdkEnv) {
            sh "docker pull vito-docker.artifactory.vgt.vito.be/geotrellis_process_graph_test_helper"
            def server = Artifactory.server('vitoartifactory' )
            server.credentialsId = 'BobDeBouwerArtifactory'

            def rtMaven = Artifactory.newMavenBuild()
            def snapshotRepo = 'libs-snapshot-public'
            def releaseRepo = 'libs-release-public'
            if (!publishable_branches.contains(env.BRANCH_NAME)) {
                snapshotRepo = 'openeo-branch-builds'
                //releaseRepo = 'openeo-branch-builds'
                rtMaven.opts += " -Drevision=${env.BRANCH_NAME}"
            }
            rtMaven.deployer server: server, releaseRepo: releaseRepo, snapshotRepo: snapshotRepo
            rtMaven.tool = maven
            if (skipTests) {
                print "Maven will skip all tests"
                rtMaven.opts += ' -DskipTests=true'
            } else if (skipSentinelHubTests) {
                print "Maven will only skip Sentinel Hub tests"
                rtMaven.opts += ' -DskipSentinelHubTests=true'
            } else {
                print "Maven will run all tests"
            }

            rtMaven.deployer.deployArtifacts = true
            //use '--projects StatisticsMapReduce' in 'goals' to build specific module
            try {
                withCredentials([
                        [$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'SentinelHubBatchS3'],
                        [$class: 'UsernamePasswordMultiBinding', credentialsId: 'SentinelHubGeodatadev', usernameVariable: 'SENTINELHUB_CLIENT_ID', passwordVariable: 'SENTINELHUB_CLIENT_SECRET']
                ]) {
                    buildInfo = rtMaven.run pom: 'pom.xml', goals: '-P default,wmts,integrationtests -U clean install' + rtMaven.opts
                    try {
                        if (rtMaven.deployer.deployArtifacts)
                            server.publishBuildInfo buildInfo
                    } catch (e) {
                        print e.message
                    }
                }
            } catch(err){
                notification.fail()

                throw err
            }
            finally {
                if (!skipTests) {
                    junit '*/target/*-reports/*.xml'
                }
                sh "chown -R jenkins:vito ."
            }
        }
    }
}

void withMavenEnv(List envVars = [], def body) {
    String mvntool = tool name: maven, type: 'hudson.tasks.Maven$MavenInstallation'
    String jdktool = tool name: "OpenJDK 11 Centos7", type: 'hudson.model.JDK'

    List mvnEnv = ["PATH+MVN=${mvntool}/bin", "PATH+JDK=${jdktool}/bin", "JAVA_HOME=${jdktool}", "MAVEN_HOME=${mvntool}"]

    mvnEnv.addAll(envVars)
    withEnv(mvnEnv) {
        body.call()
    }
}

void replacePlaceholdersAndPut(replacements = [:], templateFile, outputFile) {
    sedScripts = replacements
            .collect {
                placeholder = it.key.replaceAll("/", "\\\\/")
                value = it.value.replaceAll("/", "\\\\/")
                "-e 's/$placeholder/$value/g'"
            }
            .join(" ")

    command = "sed ${sedScripts} ${templateFile} | hdfs dfs -copyFromLocal -f - ${outputFile}"

    sh command
}
