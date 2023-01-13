package org.apache.kyuubi.jdbc.hive.auth.aws;

import java.util.HashMap;
import java.util.Map;
import org.apache.kyuubi.jdbc.hive.JdbcConnectionParams;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileProviderCredentialsContext;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.services.sso.auth.SsoProfileCredentialsProviderFactory;

public class AwsAuthent {

  public static Map<String, String> enrichSsoAws(String awsRole) {
    HashMap<String, String> awsMap = new HashMap();
    ProfileFile profileFile = ProfileFile.defaultProfileFile();
    profileFile
        .getSection("profiles", awsRole)
        .ifPresent(
            profile -> {
              ProfileProviderCredentialsContext profileProvider =
                  ProfileProviderCredentialsContext.builder()
                      .profile(profile)
                      .profileFile(profileFile)
                      .build();
              AwsSessionCredentials awsCredentials =
                  (AwsSessionCredentials)
                      new SsoProfileCredentialsProviderFactory()
                          .create(profileProvider)
                          .resolveCredentials();
              awsMap.put(
                  "spark.hadoop.fs.s3a.aws.credentials.provider",
                  "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider");
              awsMap.put("spark.hadoop.fs.s3a.access.key", awsCredentials.accessKeyId());
              awsMap.put("spark.hadoop.fs.s3a.secret.key", awsCredentials.secretAccessKey());
              awsMap.put("spark.hadoop.fs.s3a.session.token", awsCredentials.sessionToken());
            });
    return awsMap;
  }

  public static Map<String, String> enrichAws(String awsProvider, Map<String, String> sessionVars) {
    if ("sso".equalsIgnoreCase(awsProvider))
      return AwsAuthent.enrichSsoAws(sessionVars.get(JdbcConnectionParams.AWS_PROFILE));
    throw new RuntimeException("Unknown aws provider");
  }
}
