let
  pkgs = import ./nix;
in
pkgs.stdenv.mkDerivation {
  name = "crdb";
  buildInputs = (
    (with pkgs; [
      cargo-bolero
      cargo-nextest
      chromedriver
      geckodriver
      just
      niv
      samply
      sqlx-cli
      trunk
      wasm-bindgen-cli
      wasm-pack

      (fenix.combine (with fenix; [
        minimal.cargo
        minimal.rustc
        complete.rust-src
        rust-analyzer
        targets.wasm32-unknown-unknown.latest.rust-std
      ]))
    ])
  );
  SQLX_OFFLINE="true"; # for rust-analyzer
}
