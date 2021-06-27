let Scala = ./dependencies/Scala.dhall

in  { files = Scala.files Scala.Files::{repo="capsul"} }
