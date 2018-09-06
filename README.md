<div align="center">
    <img src="https://raw.githubusercontent.com/Morphl-Project/media-kit/master/05%20-%20Banners/morphl-banner-color.png" style="width:1200px; height: auto;" />
</div>

# What is MorphL

MorphL is an open-source initiative that uses machine learning to predict users' behavior in mobile & web applications and enable personalized user experiences to increase engagement & conversion rates. Morphl.io is funded through [Google Digital News Initiative](https://newsinitiative.withgoogle.com/dnifund/) (€ 50,000 grant).

The product development process usually consists of a few important phases: <strong>planning</strong>, <strong>design</strong>, <strong>development</strong> and <strong>launch</strong>. To increase engagement and conversion rates, this process undergoes multiple iterations which developers seldom navigate by looking at the data. Usually there’s somebody else, be it a product owner, marketing or sales person, analyzing it and feeding developers a feature list needed for the next product release.

There lies the gap between developers, marketers and users which leads to lots of guess-work. Consequently, the product development feedback loop is broken.

MorphL uses machine learning to optimize user interactions by <strong>analyzing product micro-metrics</strong> and enabling automatic user interface adaptation to provide a <strong>personalized user experience</strong>.

# How it works

<table>
    <tr>
        <td><img src="http://morphl.io/images/icons/icon-integrations.svg" width="120"/></td><td><strong>Morphl Integrations</strong><br/>
We integrate with Google Analytics, Facebook Ads, Mixpanel, Kissmetrics and other platforms to identify user behaviors.</td>
    </tr>
    <tr>
        <td><img src="http://morphl.io/images/icons/icon-predictive-models.svg" width="120"/></td><td><strong>Morphl Predictive Models</strong><br/>
We're utilizing open-source machine learning algorithms to build predictive models which are then used to develop predictive applications.</td>
    </tr>
    <tr>
        <td><img src="http://morphl.io/images/icons/icon-personalized-experience.svg" width="120"/></td><td><strong>Morphl Personalized Experience</strong><br/>
Developers can integrate micro-metrics directly in the user-facing product components and use the MorphL insights to build personalized user experiences.</td>
    </tr>
</table>

# Get early access

On-premises, Cloud, or Hybrid. We offer several deployment options, giving you the flexibility to leverage MorphL based on the model that best suits your business needs and your budget.

If you run a business and are planning on using MorphL in a revenue-generating product, it makes business sense to sponsor MorphL development: it ensures the project that your product relies on stays healthy and actively maintained. It can also help your exposure in the open-source community and makes it easier to attract developers.

For enterprise sales or partnerships please contact us [here](https://morphl.io/contact.html) or at contact [at] morphl.io.

# Architecture

The MorphL Platform consists of two main components:

- **[MorphL Platform Orchestrator](orchestrator/)** - This is the backbone of the platform. It sets up the infrastructure required for running pipelines for each model.

- **[MorphL Pipelines](pipelines/)** - Consists of various Python scripts, required for retrieving data from various sources, pre-processing, training a model and generating predictions.

---

The code that you'll find in this repository is a mirror that we use for making releases. If you want to contribute to a pipeline or create a new model, please open a pull request in the corresponding repository from the [MorphL-AI organization](https://github.com/Morphl-AI).  


This is project is currently under development. You can read more about it here: https://morphl.io. Follow us on Twitter: https://twitter.com/morphlio. Join our Slack community and chat with other developers: http://bit.ly/morphl-slack

## License

Licensed under the [Apache-2.0 License](https://opensource.org/licenses/Apache2.0).
