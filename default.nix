let
    pkgs = import <nixpkgs> {};
  in
  {  stdenv ? pkgs.stdenv
    ,python3 ? pkgs.python3
  }:

  stdenv.mkDerivation {
    name = "python-nix";
    version = "0.0.0.0";
    src = ./.;
    buildInputs = with pkgs; [
      python3
      python34Packages.pygit2
      python34Packages.sqlalchemy9
      python34Packages.psycopg2
      python34Packages.requests
    ];
    PYTHONPATH=./.;
    shellHook = ''
      echo -e "\nLoading nix-shell with python3 deps..."
    '';
  }
