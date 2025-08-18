package models

// PathConfig contains model path configuration
type PathConfig struct {
	External        PathsConfig `yaml:"external"`
	Transformations PathsConfig `yaml:"transformations"`
}

// PathsConfig contains multiple paths for a model type
type PathsConfig struct {
	Paths []string `yaml:"paths"`
}

// SetDefaults sets default paths if not configured
func (p *PathConfig) SetDefaults() {
	// If no external paths configured, use default
	if len(p.External.Paths) == 0 {
		p.External.Paths = []string{"models/external"}
	}

	// If no transformation paths configured, use default
	if len(p.Transformations.Paths) == 0 {
		p.Transformations.Paths = []string{"models/transformations"}
	}
}

// GetExternalPaths returns all configured external model paths
func (p *PathConfig) GetExternalPaths() []string {
	if len(p.External.Paths) == 0 {
		return []string{"models/external"}
	}
	return p.External.Paths
}

// GetTransformationPaths returns all configured transformation model paths
func (p *PathConfig) GetTransformationPaths() []string {
	if len(p.Transformations.Paths) == 0 {
		return []string{"models/transformations"}
	}
	return p.Transformations.Paths
}
