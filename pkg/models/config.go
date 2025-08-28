package models

// Config represents the complete coordinator configuration
type Config struct {
	External       ExternalConfig       `yaml:"external"`
	Transformation TransformationConfig `yaml:"transformations"`
}

// ExternalConfig defines configuration for external models
type ExternalConfig struct {
	Paths           []string `yaml:"paths"`
	DefaultDatabase string   `yaml:"defaultDatabase"`
}

// TransformationConfig defines configuration for transformation models
type TransformationConfig struct {
	Paths           []string `yaml:"paths"`
	DefaultDatabase string   `yaml:"defaultDatabase"`
}

// Validate validates and sets defaults for the configuration
func (c *Config) Validate() error {
	c.External.SetDefaults()
	c.Transformation.SetDefaults()

	return nil
}

// SetDefaults sets default paths for external models
func (c *ExternalConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/external"}
	}
}

// SetDefaults sets default paths for transformation models
func (c *TransformationConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/transformations"}
	}
}
