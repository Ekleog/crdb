let
  pkgs = import ./nix;
in
pkgs.stdenv.mkDerivation {
  name = "crdb";
  buildInputs = (
    (with pkgs; [
      cargo-bolero
      cargo-hack
      cargo-nextest
      cargo-udeps
      # chromedriver # TODO(misc-low): test for both gecko and chrome in ci and locally
      # chromium
      firefox
      geckodriver
      just
      niv
      samply
      sqlx-cli
      trunk
      wasm-bindgen-cli
      wasm-pack
      watchexec

      (fenix.combine (with fenix; [
        stable.cargo
        stable.rustc
        stable.clippy
        stable.rust-src
        rust-analyzer
        targets.wasm32-unknown-unknown.stable.rust-std
      ]))
    ])
  );
  SQLX_OFFLINE="true"; # for rust-analyzer
}
