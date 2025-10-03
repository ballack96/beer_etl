// Update the existing recipes collection schema to accept BeerXML mash step types
use("brewlytix");

// Drop the existing collection to remove the old schema
db.recipes.drop();

// Recreate the collection with the updated schema
db.createCollection("recipes", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "org_id",
        "name",
        "type",
        "batch",
        "fermentables",
        "hops",
        "yeasts",
        "audit",
      ],
      additionalProperties: true,
      properties: {
        org_id: { bsonType: "string", minLength: 1 },
        name: { bsonType: "string", minLength: 1 },
        version: { bsonType: "int", minimum: 1 },
        type: { enum: ["All Grain", "Extract", "Partial Mash"] },

        style: {
          bsonType: "object",
          additionalProperties: true,
          properties: {
            _ref: {
              bsonType: "object",
              properties: {
                catalog_styles_id: { bsonType: ["string", "objectId", "null"] },
              },
            },
            name: { bsonType: "string" },
            category: { bsonType: "string" },
            style_guides: { bsonType: "array", items: { bsonType: "string" } },
            og_range: {
              bsonType: "object",
              properties: {
                min: { bsonType: "double" },
                max: { bsonType: "double" },
              },
            },
            fg_range: {
              bsonType: "object",
              properties: {
                min: { bsonType: "double" },
                max: { bsonType: "double" },
              },
            },
            ibu_range: {
              bsonType: "object",
              properties: {
                min: { bsonType: "double" },
                max: { bsonType: "double" },
              },
            },
            srm_range: {
              bsonType: "object",
              properties: {
                min: { bsonType: "double" },
                max: { bsonType: "double" },
              },
            },
          },
        },

        brewer: { bsonType: ["string", "null"] },

        batch: {
          bsonType: "object",
          required: ["target_volume_L", "boil_time_min", "efficiency_pct"],
          properties: {
            target_volume_L: { bsonType: "double", minimum: 0 },
            boil_volume_L: { bsonType: ["double", "null"], minimum: 0 },
            boil_time_min: { bsonType: "int", minimum: 0 },
            efficiency_pct: { bsonType: "int", minimum: 0, maximum: 100 },
          },
        },

        fermentables: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["name", "amount_g"],
            additionalProperties: true,
            properties: {
              _ref: {
                bsonType: "object",
                properties: {
                  catalog_fermentables_id: {
                    bsonType: ["string", "objectId", "null"],
                  },
                },
              },
              name: { bsonType: "string" },
              amount_g: { bsonType: "double", minimum: 0 },
              yields_potential_sg: {
                bsonType: ["double", "null"],
                minimum: 1.0,
              },
              color_srm: { bsonType: ["double", "null"], minimum: 0 },
              type: {
                enum: [
                  "Grain",
                  "Sugar",
                  "Extract",
                  "Dry Extract",
                  "Adjunct",
                  null,
                ],
              },
              late_addition: { bsonType: ["bool", "null"] },
              origin: { bsonType: ["string", "null"] },
              diastatic_power_Lintner: {
                bsonType: ["double", "null"],
                minimum: 0,
              },
              notes: { bsonType: ["string", "null"] },
              original: {
                bsonType: ["object", "null"],
                properties: {
                  amount_lb: { bsonType: ["double", "null"] },
                },
              },
            },
          },
        },

        hops: {
          bsonType: "array",
          items: {
            bsonType: "object",
            required: ["name", "amount_g", "use"],
            properties: {
              _ref: {
                bsonType: "object",
                properties: {
                  catalog_hops_id: { bsonType: ["string", "objectId", "null"] },
                },
              },
              name: { bsonType: "string" },
              alpha_acid_pct: {
                bsonType: ["double", "null"],
                minimum: 0,
                maximum: 40,
              },
              form: { enum: ["Pellet", "Leaf", "Plug", null] },
              use: {
                enum: [
                  "Mash",
                  "Boil",
                  "First Wort",
                  "Aroma",
                  "Dry Hop",
                  "Whirlpool",
                ],
              },
              time_min: { bsonType: ["int", "double", "null"], minimum: 0 },
              amount_g: { bsonType: "double", minimum: 0 },
              origin: { bsonType: ["string", "null"] },
              notes: { bsonType: ["string", "null"] },
            },
          },
        },

        yeasts: {
          bsonType: "array",
          minItems: 1,
          items: {
            bsonType: "object",
            required: ["name", "type", "form"],
            properties: {
              _ref: {
                bsonType: "object",
                properties: {
                  catalog_yeasts_id: {
                    bsonType: ["string", "objectId", "null"],
                  },
                },
              },
              name: { bsonType: "string" },
              type: { enum: ["Ale", "Lager", "Wheat", "Wine", "Champagne"] },
              form: { enum: ["Liquid", "Dry", "Slant", "Culture"] },
              attenuation_pct: {
                bsonType: ["int", "null"],
                minimum: 0,
                maximum: 100,
              },
              min_temp_C: { bsonType: ["double", "null"] },
              max_temp_C: { bsonType: ["double", "null"] },
              amount_cells_billion: {
                bsonType: ["double", "null"],
                minimum: 0,
              },
              notes: { bsonType: ["string", "null"] },
            },
          },
        },

        miscs: {
          bsonType: ["array", "null"],
          items: {
            bsonType: "object",
            required: ["name", "type", "use", "time_min", "amount_g"],
            properties: {
              name: { bsonType: "string" },
              type: { bsonType: "string" }, // Fining, Spice, Water Agent, etc.
              use: {
                enum: ["Boil", "Mash", "Primary", "Secondary", "Bottling"],
              },
              time_min: { bsonType: ["int", "double"], minimum: 0 },
              amount_g: { bsonType: ["double", "int"], minimum: 0 },
              notes: { bsonType: ["string", "null"] },
            },
          },
        },

        water_profile: {
          bsonType: ["object", "null"],
          properties: {
            _ref: {
              bsonType: "object",
              properties: {
                catalog_waters_id: { bsonType: ["string", "objectId", "null"] },
              },
            },
            name: { bsonType: ["string", "null"] },
            calcium_ppm: { bsonType: ["double", "null"], minimum: 0 },
            magnesium_ppm: { bsonType: ["double", "null"], minimum: 0 },
            sodium_ppm: { bsonType: ["double", "null"], minimum: 0 },
            chloride_ppm: { bsonType: ["double", "null"], minimum: 0 },
            sulfate_ppm: { bsonType: ["double", "null"], minimum: 0 },
            bicarbonate_ppm: { bsonType: ["double", "null"], minimum: 0 },
            ph: { bsonType: ["double", "null"], minimum: 0, maximum: 14 },
            notes: { bsonType: ["string", "null"] },
          },
        },

        mash: {
          bsonType: ["object", "null"],
          properties: {
            _ref: {
              bsonType: "object",
              properties: {
                catalog_mash_profiles_id: {
                  bsonType: ["string", "objectId", "null"],
                },
              },
            },
            name: { bsonType: ["string", "null"] },
            sparge_temp_C: { bsonType: ["double", "null"] },
            ph: { bsonType: ["double", "null"], minimum: 0, maximum: 14 },
            steps: {
              bsonType: ["array", "null"],
              items: {
                bsonType: "object",
                required: ["name", "type", "step_temp_C", "step_time_min"],
                properties: {
                  name: { bsonType: "string" },
                  type: { 
                    enum: [
                      "Infusion", 
                      "Temperature", 
                      "Decoction",
                      "Strike",
                      "Mash In",
                      "Mash Out", 
                      "Sparge",
                      "Fly Sparge",
                      "Batch Sparge",
                      "Rinse",
                      "Drain Mash Tun",
                      "Add Water"
                    ] 
                  },
                  step_temp_C: { bsonType: "double" },
                  step_time_min: { bsonType: ["int", "double"], minimum: 0 },
                  infuse_amount_L: { bsonType: ["double", "null"], minimum: 0 },
                },
              },
            },
          },
        },

        equipment: {
          bsonType: ["object", "null"],
          properties: {
            _ref: {
              bsonType: "object",
              properties: {
                catalog_equipment_id: {
                  bsonType: ["string", "objectId", "null"],
                },
              },
            },
            name: { bsonType: ["string", "null"] },
            boil_off_rate_L_per_hr: {
              bsonType: ["double", "null"],
              minimum: 0,
            },
            trub_loss_L: { bsonType: ["double", "null"], minimum: 0 },
            lauter_deadspace_L: { bsonType: ["double", "null"], minimum: 0 },
          },
        },

        estimates: {
          bsonType: ["object", "null"],
          properties: {
            og: { bsonType: ["double", "null"], minimum: 0.99, maximum: 1.2 },
            fg: { bsonType: ["double", "null"], minimum: 0.99, maximum: 1.2 },
            ibu: {
              bsonType: ["object", "null"],
              properties: {
                method: { enum: ["Tinseth", "Rager", "Garetz", null] },
                value: { bsonType: ["double", "null"], minimum: 0 },
              },
            },
            color: {
              bsonType: ["object", "null"],
              properties: {
                srm: { bsonType: ["double", "null"], minimum: 0 },
                ebc: { bsonType: ["double", "null"], minimum: 0 },
              },
            },
            abv_pct: { bsonType: ["double", "null"], minimum: 0, maximum: 30 },
          },
        },

        instructions: {
          bsonType: ["object", "null"],
          properties: {
            mash_notes: { bsonType: ["string", "null"] },
            boil_notes: { bsonType: ["string", "null"] },
            fermentation: {
              bsonType: ["array", "null"],
              items: {
                bsonType: "object",
                required: ["stage", "temp_C", "time_days"],
                properties: {
                  stage: { bsonType: "int", minimum: 1 },
                  temp_C: { bsonType: "double" },
                  time_days: { bsonType: ["int", "double"], minimum: 0 },
                },
              },
            },
            packaging: {
              bsonType: ["object", "null"],
              properties: {
                priming_sugar_g: { bsonType: ["double", "null"], minimum: 0 },
                target_co2_vols: { bsonType: ["double", "null"], minimum: 0 },
              },
            },
          },
        },

        labels: { bsonType: ["array", "null"], items: { bsonType: "string" } },

        provenance: {
          bsonType: ["object", "null"],
          properties: {
            beerxml_version: { bsonType: ["int", "null"], minimum: 1 },
            source_uri: { bsonType: ["string", "null"] },
            source_record_id: { bsonType: ["string", "null"] },
            imported_at: { bsonType: ["date", "string", "null"] },
            original_xml_fragment: { bsonType: ["string", "null"] },
            checksum_sha256: { bsonType: ["string", "null"] },
          },
        },

        ai: {
          bsonType: ["object", "null"],
          properties: {
            embedding_v1: {
              bsonType: ["object", "null"],
              properties: {
                dim: { bsonType: ["int", "null"] },
                model: { bsonType: ["string", "null"] },
                vec: {
                  bsonType: ["array", "null"],
                  items: { bsonType: "double" },
                },
              },
            },
            normalized_tokens: {
              bsonType: ["array", "null"],
              items: { bsonType: "string" },
            },
          },
        },

        audit: {
          bsonType: "object",
          required: ["created_at"],
          properties: {
            created_by: { bsonType: ["string", "null"] },
            created_at: { bsonType: ["date", "string"] },
            updated_at: { bsonType: ["date", "string", "null"] },
          },
        },
      },
    },
  },
});

// Recreate the indexes
db.recipes.createIndex({ org_id: 1, name: 1, version: -1 });
db.recipes.createIndex({ "style.name": 1 });
db.recipes.createIndex({ "estimates.abv_pct": 1 });
db.recipes.createIndex({ labels: 1 });
db.recipes.createIndex({
  name: "text",
  brewer: "text",
  "style.name": "text",
  "fermentables.name": "text",
  "hops.name": "text",
});

print("✅ Schema updated successfully! The recipes collection now accepts BeerXML mash step types.");
