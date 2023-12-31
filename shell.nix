let
  pkgs = import ./nix;
in
pkgs.stdenv.mkDerivation {
  name = "crdb";
  buildInputs = (
    (with pkgs; [
      cargo-bolero
      cargo-nextest
      just
      niv
      samply
      sqlx-cli
      trunk

      (fenix.combine (with fenix; [
        minimal.cargo
        minimal.rustc
        rust-analyzer
        targets.wasm32-unknown-unknown.latest.rust-std
      ]))
    ])
  );
  SQLX_OFFLINE="true"; # for rust-analyzer
}
