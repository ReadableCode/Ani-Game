# %%


def transform_url(original_url):
    transformed_url = ""
    if original_url.startswith("https://hooks.slack.com/services/"):
        transformed_url = original_url.split("https://hooks.slack.com/services/")[1]
        transformed_url = transformed_url.replace("/", "%2F")
        transformed_url = transformed_url.replace(":", "%3A")
        transformed_url = (
            "http://:" + transformed_url + "@https%3A%2F%2Fhooks.slack.com%2Fservices"
        )

    return transformed_url


# %%
