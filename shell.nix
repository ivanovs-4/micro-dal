let
  release = import ./release.nix;
  pkgs = release.pkgs;
in pkgs.haskellPackages.shellFor {
  nativeBuildInputs = with pkgs.haskellPackages; [
    cabal-install
    ghcid
  ];
  buildInputs = with pkgs; [
                      minio
                    ];
  packages = _: pkgs.lib.attrValues release.packages;

  shellHook = ''
      export LANG=C
      export LC_ALL=C
      function cleanup {
        kill 0
      }
      # https://stackoverflow.com/questions/360201/how-do-i-kill-background-processes-jobs-when-my-shell-script-exits
      trap exit INT TERM
      trap cleanup EXIT

      export TEST_S3_ADDR="http://127.0.0.1:9001"
      export TEST_S3_ACCESS_KEY="s3-access-key"
      export TEST_S3_SECRET_KEY="s3-secret-key"
      export TEST_S3_BUCKET="test"

      minio --compat server --address :9001 ./storage > /dev/null &
      sleep 1
    '';

}
