ThisBuild / version := sys.props.getOrElse("odp.delta.version", "3.3.1") + "." + sys.props.getOrElse("odp.release.version", "3.4.3.0-SNAPSHOT")
