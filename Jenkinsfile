@Library('lib')_

def deployable_branches = ["master"]
maven = 'Maven 3.5.4'


node ('devdmz') {
    stage('Build and Test') {
        sh "rm -rf *"
        sh "rm -rf .git/"
        checkout scm
        rel_version = getMavenVersion()
        build()
    }

    if(["master","develop"].contains(env.BRANCH_NAME)) {
        stage('Deploy to Dev') {
            //milestone ensures that previous builds that have reached this point are aborted
            milestone()           

        }
    }
}

if(deployable_branches.contains(env.BRANCH_NAME)){
    stage('Input'){
        milestone()
        input "Release build ${rel_version}?"
        milestone()
    }

    node('devdmz'){
        stage('Releasing'){
            checkout scm
            rel_version = getReleaseVersion()
            withMavenEnv(["JAVA_OPTS=-Xmx1536m -Xms512m","HADOOP_CONF_DIR=/etc/hadoop/conf/"]){
                sh "mvn versions:use-releases -DgenerateBackupPoms=false -DfailIfNotReplaced=true"
                echo "Removing SNAPSHOT from version for release"
                sh "mvn versions:set -DgenerateBackupPoms=false -DnewVersion=${rel_version}"
            }
            echo "releasing version ${rel_version}"
            build(tests = false)

            withMavenEnv(["JAVA_OPTS=-Xmx1536m -Xms512m","HADOOP_CONF_DIR=/etc/hadoop/conf/"]){
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
    def publishable_branches = ["master","develop", "feature/spark3"]
    String jdktool = tool name: "OpenJDK 8 Centos7", type: 'hudson.model.JDK'
    List jdkEnv = ["PATH+JDK=${jdktool}/bin", "JAVA_HOME=${jdktool}", "HADOOP_CONF_DIR=/etc/hadoop/conf/","SPARK_LOCAL_IP=127.0.0.1"]
    withEnv(jdkEnv) {
        def server = Artifactory.server('vitoartifactory')
        def rtMaven = Artifactory.newMavenBuild()
        rtMaven.deployer server: server, releaseRepo: 'libs-release-public', snapshotRepo: 'libs-snapshot-public'
        rtMaven.tool = maven
        if (!tests) {
            rtMaven.opts += ' -DskipTests=true'
        }
        rtMaven.deployer.deployArtifacts = publishable_branches.contains(env.BRANCH_NAME)
        //use '--projects StatisticsMapReduce' in 'goals' to build specific module
        try {
            withCredentials([
                    [$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'SentinelHubBatchS3'],
                    [$class: 'UsernamePasswordMultiBinding', credentialsId: 'SentinelHubGeodatadev', usernameVariable: 'SENTINELHUB_CLIENT_ID', passwordVariable: 'SENTINELHUB_CLIENT_SECRET']
            ]) {
                buildInfo = rtMaven.run pom: 'pom.xml', goals: '-P default,wmts -U clean install'
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

void withMavenEnv(List envVars = [], def body) {
    String mvntool = tool name: maven, type: 'hudson.tasks.Maven$MavenInstallation'
    String jdktool = tool name: "OpenJDK 8 Centos7", type: 'hudson.model.JDK'

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
