Feature: Create a step
  APF has a command line interface allowing to create steps
  There are 3 types of steps: simple, component and composite
  Creating a step with the CLI will provide all the
  required boilerplate for writing a step

  Scenario: Create a simple step
    When user calls the new-step command
    Then a new directory is created with the step boilerplate
