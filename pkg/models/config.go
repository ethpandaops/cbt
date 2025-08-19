package models

// Config represents the complete coordinator configuration
type Config struct {
	External       ExternalPathsConfig       `yaml:"external"`
	Transformation TransformationPathsConfig `yaml:"transformation"`
}

type ExternalPathsConfig struct {
	Paths []string `yaml:"paths"`
}

type TransformationPathsConfig struct {
	Paths []string `yaml:"paths"`
}

func (c *Config) Validate() error {
	c.External.SetDefaults()
	c.Transformation.SetDefaults()

	return nil
}

func (c *ExternalPathsConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/external"}
	}
}

func (c *TransformationPathsConfig) SetDefaults() {
	if len(c.Paths) == 0 {
		c.Paths = []string{"models/transformations"}
	}
}
