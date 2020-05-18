{ mkDerivation, aeson, base, base64-bytestring, binary, bytestring
, case-insensitive, conduit, conduit-extra, connection, cryptonite
, cryptonite-conduit, digest, directory, exceptions, fetchgit
, filepath, http-client, http-client-tls, http-conduit, http-types
, ini, memory, protolude, QuickCheck, raw-strings-qq, resourcet
, retry, stdenv, tasty, tasty-hunit, tasty-quickcheck
, tasty-smallcheck, text, time, transformers, unliftio
, unliftio-core, unordered-containers, xml-conduit
}:
mkDerivation {
  pname = "minio-hs";
  version = "1.5.2";
  src = fetchgit {
    url = "https://github.com/hexresearch/minio-hs.git";
    sha256 = "12ma8scrwfvl1wmq8lmpazlsnzqhxk25syrcljcyrh013b796rpx";
    rev = "346f2838fd2388632bebe94e70514fcd243fa5e4";
    fetchSubmodules = true;
  };
  isLibrary = true;
  isExecutable = true;
  libraryHaskellDepends = [
    aeson base base64-bytestring binary bytestring case-insensitive
    conduit conduit-extra connection cryptonite cryptonite-conduit
    digest directory exceptions filepath http-client http-client-tls
    http-conduit http-types ini memory protolude raw-strings-qq
    resourcet retry text time transformers unliftio unliftio-core
    unordered-containers xml-conduit
  ];
  testHaskellDepends = [
    aeson base base64-bytestring binary bytestring case-insensitive
    conduit conduit-extra connection cryptonite cryptonite-conduit
    digest directory exceptions filepath http-client http-client-tls
    http-conduit http-types ini memory protolude QuickCheck
    raw-strings-qq resourcet retry tasty tasty-hunit tasty-quickcheck
    tasty-smallcheck text time transformers unliftio unliftio-core
    unordered-containers xml-conduit
  ];
  homepage = "https://github.com/minio/minio-hs#readme";
  description = "A MinIO Haskell Library for Amazon S3 compatible cloud storage";
  license = stdenv.lib.licenses.asl20;
}
