@Library('lib')_

def deployable_branches = ["master"]
maven = 'system_maven'
def config = [:]

def build_container_image    = (config.build_container_image == true) ?: false

def docker_registry_dev      = config.docker_registry_dev ?: globalDefaults.docker_registry_dev()
def docker_registry_prod     = config.docker_registry_prod ?: globalDefaults.docker_registry_prod()
def jdk_version              = 11
def maven_version            =  '3.5.4'
def node_label               =  'default'
def wipeout_workspace        =  true

def maven_image              = "vito-docker.artifactory.vgt.vito.be/almalinux8.5-spark-py-openeo:3.4.0"


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
        JDK_VERSION = "${jdk_version}"
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
                    build( !params.skip_tests)
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
                    build(tests = false)

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

void build(tests = true){
    def publishable_branches = ["master", "develop", "109-upgrade-to-spark-33"]

    List jdkEnv = [ "SPARK_LOCAL_IP=127.0.0.1", "JAVA_HOME=/usr/lib/jvm/java-11-openjdk"]
    docker.image(env.MAVEN_IMAGE).inside('-v /home/jenkins/.m2:/root/.m2 -v /etc/hadoop/conf:/etc/hadoop/conf:ro -v /data:/data:ro -u root') {
        withEnv(jdkEnv) {
            sh "dnf install -y maven git java-11-openjdk-devel gdal-3.8.4"
            def server = Artifactory.server('vitoartifactory')
            def rtMaven = Artifactory.newMavenBuild()
            def snapshotRepo = 'libs-snapshot-public'
            if (!publishable_branches.contains(env.BRANCH_NAME)) {
                snapshotRepo = 'openeo-branch-builds'
                rtMaven.opts += " -Drevision=${env.BRANCH_NAME}"
            }
            rtMaven.deployer server: server, releaseRepo: 'libs-release-public', snapshotRepo: snapshotRepo
            rtMaven.tool = maven
            if (!tests) {
                rtMaven.opts += ' -DskipTests=true'
            }
            rtMaven.deployer.deployArtifacts = true
            //use '--projects StatisticsMapReduce' in 'goals' to build specific module
            try {
                withCredentials([
                        [$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'SentinelHubBatchS3'],
                        [$class: 'UsernamePasswordMultiBinding', credentialsId: 'SentinelHubGeodatadev', usernameVariable: 'SENTINELHUB_CLIENT_ID', passwordVariable: 'SENTINELHUB_CLIENT_SECRET']
                ]) {
                    buildInfo = rtMaven.run pom: 'pom.xml', goals: '-P default,wmts,integrationtests -U clean install'
                    try {
                        if (rtMaven.deployer.deployArtifacts)
                            server.publishBuildInfo buildInfo
                    } catch (e) {
                        print e.message
                    }
                }
            }catch(err){
                notification.fail()

                throw err
            }
            finally {
                if (tests) {
                    junit '*/target/*-reports/*.xml'
                }
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
