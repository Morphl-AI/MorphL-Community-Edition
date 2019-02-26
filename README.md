<div align="center">
    <img src="https://raw.githubusercontent.com/Morphl-Project/media-kit/master/05%20-%20Banners/morphl-banner-color.png" style="width:1200px; height: auto;" />
</div>

# MorphL Community Edition

MorphL Community Edition uses Big Data & Machine Learning to predict user behaviors in digital products and services with the goal of increasing KPIs (click-through rates, conversion rates, etc.) through personalization. MorphL AI is funded through [Google Digital News Initiative](https://newsinitiative.withgoogle.com/dnifund/) and [European Data Incubator](https://edincubator.eu/).

The process of building successful data-driven products undergoes many iterations. Data scientists, product manager, marketing or sales people and software developers need to come together to analyze the data and create a feature list for the next product release. This leads to lots of guess-work, not to mention the huge amount of time and resources required to reach a decent result, whether that’s spent on analyzing the data or developing new or improved product features.

MorphL reduces the complexity of implementing a **personalized digital experience** by offering built-in ML models & algorithms that cover a wide range of data sources and use-cases.

# How it works

<table>
    <tr>
        <td><img src="https://morphl.io/images/icons/analytics/file-2.svg" width="120"/></td><td><strong>MorphL Platform</strong><br/>
        The backbone of the platform is the <strong>MorphL Orchestrator</strong>, that sets up the BigData techstack required for running pipelines for data ingestion, models training and generating predictions.
        </td>
    </tr>
    <tr>
        <td><img src="https://morphl.io/images/icons/analytics/server.svg" width="120"/></td><td><strong>MorphL Integrations</strong><br/>
We integrate with various data sources. At the moment, we support Google Analytics, Google Analytics 360, BigQuery, Google Cloud Storage and AWS S3.</td>
    </tr>
    <tr>
        <td><img src="https://morphl.io/images/icons/analytics/analytics-1.svg" width="120"/></td><td><strong>MorphL Predictive Models</strong><br/>
We're utilizing open-source machine learning algorithms to build predictive models which are then used to develop predictive applications.</td>
    </tr>
    <tr>
        <td><img src="https://morphl.io/images/icons/analytics/api.svg" width="120"/></td><td><strong>MorphL Predictions API</strong><br/>
        All predictions are available via a REST API, which makes it easier for software developers to incorporate AI capabilities within their digital products or services. 
    </td>
</tr>

</table>

# MorphL Cloud

On-premises, Cloud or Hybrid. For companies that want to AI-enhance their digital experience without the hassle of dealing with a BigData & Machine Learning infrastructure, we offer several deployment options, giving you the flexibility to leverage MorphL based on the model that best suits your business needs and your budget.

If you run a business and are planning on using MorphL in a revenue-generating product, it makes business sense to sponsor MorphL development: it ensures the project that your product relies on stays healthy and actively maintained. It can also help your exposure in the open-source community and makes it easier to attract developers.

For enterprise sales or partnerships please contact us [here](https://morphl.io/company/contact.html) or at contact [at] morphl.io.

# Architecture

The MorphL Platform consists of two main components:

- **[MorphL Platform Orchestrator](orchestrator/)** - This is the backbone of the platform. It sets up the infrastructure required for running pipelines for each model.

- **[MorphL Pipelines](pipelines/)** - Consists of various Python scripts, required for retrieving data from various sources, pre-processing, training a model and generating predictions.

---

The code that you'll find in this repository is a mirror that we use for making releases. If you want to contribute to a pipeline or create a new model, please open a pull request in the corresponding repository from the [MorphL-AI organization](https://github.com/Morphl-AI).

You can read more about MorphL here: https://morphl.io. Follow us on Twitter: https://twitter.com/morphlio. Join our Slack community and chat with other developers: http://bit.ly/morphl-slack

## License

Licensed under the [Apache-2.0 License](https://opensource.org/licenses/Apache2.0).
