package org.apache.beam.runners.flink;

/**
 * Determines artifact path names within the
 * {@link org.apache.flink.api.common.cache.DistributedCache}.
 */
public class FlinkCachedArtifactPaths {
  private final static String ARTIFACT_TEMPLATE = "ARTIFACT_%s_%s";
  private final static String DEFAULT_ARTIFACT_TOKEN = "default";
  private final static String MANIFEST_TEMPLATE = "MANIFEST_%s";

  public static FlinkCachedArtifactPaths createDefault() {
    return new FlinkCachedArtifactPaths(DEFAULT_ARTIFACT_TOKEN);
  }

  public static FlinkCachedArtifactPaths forToken(String artifactToken) {
    return new FlinkCachedArtifactPaths(artifactToken);
  }

  private final String token;

  private FlinkCachedArtifactPaths(String token) {
    this.token = token;
  }

  public String getArtifactPath(String name) {
    return String.format(ARTIFACT_TEMPLATE, token);
  }

  public String getManifestPath() {
    return String.format(MANIFEST_TEMPLATE, token);
  }
}
