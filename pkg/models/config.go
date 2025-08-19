package models

// Config represents the complete coordinator configuration
type Config struct {
	External       ExternalPathsConfig       `yaml:"external"`
	Transformation TransformationPathsConfig `yaml:"transformation"`
}

// ExternalPathsConfig defines paths for external model discovery
type ExternalPathsConfig struct {
	Paths []string `yaml:"paths"`
}

// TransformationPathsConfig defines paths for transformation model discovery
type TransformationPathsConfig struct {
	Paths []string `yaml:"paths"`
}

// Validate validates and sets defaults for the configuration
func (c *Config) Validate() error {
	c.External.SetDefaults()
	c.Transformation.SetDefaults()

	return nil
}

// SetDefaults sets default paths for external models
func (c *ExternalPathsConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/external"}
	}
}

// SetDefaults sets default paths for transformation models
func (c *TransformationPathsConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/transformations"}
	}
}
