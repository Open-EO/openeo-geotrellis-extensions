package org.openeo.workspace

object WorkspaceRepository {


    val instance = new WorkspaceRepository()

    def get(): WorkspaceRepository = instance
}

case class WorkspaceConfig(bucketName: String, bucketRegion: String, profile: Option[String], bucketEndpoint: Option[String])

class WorkspaceRepository {
    /**
     * A repository to retrieve information about workspaces.
     */

    private val workspaces = scala.collection.mutable.Map[String, WorkspaceConfig]()
    private val workspacesByBucket = scala.collection.mutable.Map[String, WorkspaceConfig]()

    def registerBucketDetails( workspaceId:String, bucketName: String,
                                bucketRegion: String,
                                bucketEndpoint: String): Unit = {
        val config = WorkspaceConfig(bucketName, bucketRegion, Option.empty[String], Some(bucketEndpoint))
        workspaces(workspaceId) = config
        workspacesByBucket(bucketName) = config
    }

    def registerBucketDetailsWithProfile( workspaceId:String, bucketName: String, bucketRegion:String,
                               profile: String): Unit = {
        val config = WorkspaceConfig(bucketName, bucketRegion, Some(profile), Option.empty[String])
        workspaces(workspaceId) = config
        workspacesByBucket(bucketName) = config
    }

    def getWorkspaceByBucket(bucketName: String): Option[WorkspaceConfig] =
    {
        workspacesByBucket.get(bucketName)
    }
}
