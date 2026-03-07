import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Key, Code, Settings, CheckCircle, BookOpen, Terminal, ChevronRight, Zap } from 'lucide-react';
import { getApiKeys } from '../services/api';
import { useToast } from '../context/ToastContext';
import './GettingStarted.css';

function GettingStarted() {
  const navigate = useNavigate();
  const [copiedStep, setCopiedStep] = useState(null);
  const [activeTab, setActiveTab] = useState('spark-submit');
  const [latestVersion, setLatestVersion] = useState('1.5.0');
  const [apiKeys, setApiKeys] = useState([]);
  const toast = useToast();

  useEffect(() => {
    fetchLatestVersion();
    fetchApiKeys();
  }, []);

  const fetchApiKeys = async () => {
    try {
      const res = await getApiKeys();
      setApiKeys(res.data);
    } catch (err) {
      console.error('Failed to load API keys:', err);
    }
  };

  const fetchLatestVersion = async () => {
    try {
      const metadataUrl = "https://repo1.maven.org/maven2/io/github/riju377/data-observability-platform_2.12/maven-metadata.xml";
      const proxy = `https://royal-bush-7f82.rijum8153.workers.dev?url=${encodeURIComponent(metadataUrl)}`;

      const res = await fetch(proxy);
      if (!res.ok) return;

      const text = await res.text();
      const parser = new DOMParser();
      const xml = parser.parseFromString(text, "application/xml");

      const latest = xml.querySelector("metadata > versioning > latest")?.textContent ||
                     xml.querySelector("versioning > latest")?.textContent ||
                     xml.querySelector("latest")?.textContent;

      if (latest) {
        setLatestVersion(latest);
      }
    } catch (e) {
      console.error('Failed to fetch latest version:', e);
    }
  };

  const handleCopy = async (text, id, label = 'Code') => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedStep(id);
      setTimeout(() => setCopiedStep(null), 2000);
      toast.success('Copied!', `${label} copied to clipboard`);
    } catch (err) {
      toast.error('Copy failed', 'Could not copy to clipboard');
    }
  };

  const getApiKeyPlaceholder = () => {
    return apiKeys.length > 0 ? apiKeys[0].key_prefix + '...' : '<YOUR_API_KEY>';
  };

  return (
    <div className="getting-started">
      <div className="welcome-banner">
        <div className="welcome-icon">
          <Zap size={48} strokeWidth={1.5} />
        </div>
        <h1>Welcome to Data Observability Platform!</h1>
        <p className="welcome-description">
          Monitor your Spark jobs, track data lineage, detect anomalies, and get alerts - all in one place.
          Get started in just a few minutes by following the steps below.
        </p>
      </div>

      <div className="steps-container">
        {/* Step 1: Generate API Key */}
        <div className="step-card">
          <div className="step-header">
            <div className="step-number">1</div>
            <div className="step-icon api-key">
              <Key size={24} />
            </div>
            <div className="step-title">
              <h3>Generate Your API Key</h3>
              <p>Create an API key to authenticate your Spark jobs</p>
            </div>
          </div>
          <div className="step-content">
            <p className="step-description">
              Your API key is used to securely send job metadata from your Spark applications to this platform.
              {apiKeys.length === 0 && " You don't have any API keys yet."}
            </p>
            <button
              className="primary-button"
              onClick={() => navigate('/api-keys')}
            >
              <Key size={18} />
              {apiKeys.length === 0 ? 'Create Your First API Key' : 'Manage API Keys'}
            </button>
            {apiKeys.length > 0 && (
              <div className="api-key-info">
                ✓ You have {apiKeys.length} API key{apiKeys.length > 1 ? 's' : ''} configured
              </div>
            )}
          </div>
        </div>

        {/* Step 2: Add the Package */}
        <div className="step-card">
          <div className="step-header">
            <div className="step-number">2</div>
            <div className="step-icon dependency">
              <Code size={24} />
            </div>
            <div className="step-title">
              <h3>Add the Package</h3>
              <p>Choose your build tool and add the listener</p>
            </div>
          </div>
          <div className="step-content">
            <p className="step-description">
              Add the Data Observability listener to your Spark project.
              <a
                href="https://central.sonatype.com/artifact/io.github.riju377/data-observability-platform_2.12"
                target="_blank"
                rel="noopener noreferrer"
                className="version-link"
              >
                Latest: {latestVersion} ↗
              </a>
            </p>

            <div className="code-tabs">
              <div className="tab-bar">
                <button
                  className={`tab-btn ${activeTab === 'spark-submit' ? 'active' : ''}`}
                  onClick={() => setActiveTab('spark-submit')}
                >
                  <Terminal size={14} />
                  spark-submit
                </button>
                <button
                  className={`tab-btn ${activeTab === 'sbt' ? 'active' : ''}`}
                  onClick={() => setActiveTab('sbt')}
                >
                  SBT
                </button>
                <button
                  className={`tab-btn ${activeTab === 'maven' ? 'active' : ''}`}
                  onClick={() => setActiveTab('maven')}
                >
                  Maven
                </button>
              </div>

              {activeTab === 'spark-submit' && (
                <div className="code-block-container">
                  <div className="code-block">
                    <pre>{`spark-submit \\
  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.sql.queryExecutionListeners=com.observability.listener.ObservabilityListener \\
  --conf spark.observability.api.key=${getApiKeyPlaceholder()} \\
  your-application.jar`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() => handleCopy(
                        `spark-submit \\\n  --packages io.github.riju377:data-observability-platform_2.12:${latestVersion} \\\n  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \\\n  --conf spark.sql.queryExecutionListeners=com.observability.listener.ObservabilityListener \\\n  --conf spark.observability.api.key=${getApiKeyPlaceholder()} \\\n  your-application.jar`,
                        'spark-submit',
                        'spark-submit command'
                      )}
                    >
                      {copiedStep === 'spark-submit' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                </div>
              )}

              {activeTab === 'sbt' && (
                <div className="code-block-container">
                  <div className="code-block">
                    <pre>{`// build.sbt
libraryDependencies += "io.github.riju377" %% "data-observability-platform" % "${latestVersion}"`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() => handleCopy(
                        `libraryDependencies += "io.github.riju377" %% "data-observability-platform" % "${latestVersion}"`,
                        'sbt',
                        'SBT dependency'
                      )}
                    >
                      {copiedStep === 'sbt' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                </div>
              )}

              {activeTab === 'maven' && (
                <div className="code-block-container">
                  <div className="code-block">
                    <pre>{`<dependency>
  <groupId>io.github.riju377</groupId>
  <artifactId>data-observability-platform_2.12</artifactId>
  <version>${latestVersion}</version>
</dependency>`}</pre>
                    <button
                      className="copy-code-btn"
                      onClick={() => handleCopy(
                        `<dependency>\n  <groupId>io.github.riju377</groupId>\n  <artifactId>data-observability-platform_2.12</artifactId>\n  <version>${latestVersion}</version>\n</dependency>`,
                        'maven',
                        'Maven dependency'
                      )}
                    >
                      {copiedStep === 'maven' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Step 3: Configure the Listener */}
        <div className="step-card">
          <div className="step-header">
            <div className="step-number">3</div>
            <div className="step-icon spark">
              <Settings size={24} />
            </div>
            <div className="step-title">
              <h3>Configure the Listener</h3>
              <p>Pass these Spark configuration flags when submitting your job</p>
            </div>
          </div>
          <div className="step-content">
            <div className="config-table">
              <div className="config-section">
                <h4>Required Configuration</h4>
                <div className="config-row">
                  <code className="config-key">spark.extraListeners</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">com.observability.listener.ObservabilityListener</code>
                </div>
                <div className="config-row">
                  <code className="config-key">spark.sql.queryExecutionListeners</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">com.observability.listener.ObservabilityListener</code>
                </div>
                <div className="config-row">
                  <code className="config-key">spark.observability.api.key</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">{getApiKeyPlaceholder()}</code>
                </div>
              </div>

              <div className="config-section">
                <h4>Optional Configuration</h4>
                <div className="config-row">
                  <code className="config-key">spark.observability.job.name</code>
                  <ChevronRight size={14} className="config-arrow" />
                  <code className="config-val">"My Job Name"</code>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Step 4: Run & Monitor */}
        <div className="step-card">
          <div className="step-header">
            <div className="step-number">4</div>
            <div className="step-icon run">
              <CheckCircle size={24} />
            </div>
            <div className="step-title">
              <h3>Run &amp; Monitor</h3>
              <p>Submit your Spark job and watch the data flow in!</p>
            </div>
          </div>
          <div className="step-content">
            <p className="step-description">
              Submit your Spark job as usual. The listener automatically captures comprehensive metrics:
            </p>

            <div className="metrics-section">
              <h4>📊 Dataset Metrics</h4>
              <div className="metrics-grid">
                <div className="metric-item">• Row counts</div>
                <div className="metric-item">• Data size (bytes)</div>
                <div className="metric-item">• Partition counts</div>
                <div className="metric-item">• File formats</div>
              </div>
            </div>

            <div className="metrics-section">
              <h4>📈 Column-Level Statistics</h4>
              <div className="metrics-grid">
                <div className="metric-item">• Null counts</div>
                <div className="metric-item">• Distinct value counts</div>
                <div className="metric-item">• Min/Max values</div>
                <div className="metric-item">• Data types</div>
              </div>
            </div>

            <div className="metrics-section">
              <h4>🔗 Lineage Tracking</h4>
              <div className="metrics-grid">
                <div className="metric-item">• Table dependencies</div>
                <div className="metric-item">• Column-level lineage</div>
                <div className="metric-item">• Transformation graph</div>
                <div className="metric-item">• Source → Target mapping</div>
              </div>
            </div>

            <div className="metrics-section">
              <h4>📋 Schema & Quality</h4>
              <div className="metrics-grid">
                <div className="metric-item">• Schema versioning</div>
                <div className="metric-item">• Column additions/removals</div>
                <div className="metric-item">• Type changes detection</div>
                <div className="metric-item">• Anomaly alerts (3σ threshold)</div>
              </div>
            </div>

            <div className="metrics-section">
              <h4>⚡ Job & Execution Metrics</h4>
              <div className="metrics-grid">
                <div className="metric-item">• Job duration & timing</div>
                <div className="metric-item">• Stage & task counts</div>
                <div className="metric-item">• Executor metrics (CPU, memory)</div>
                <div className="metric-item">• Shuffle read/write bytes</div>
                <div className="metric-item">• Records processed per stage</div>
                <div className="metric-item">• Task failures & retries</div>
              </div>
            </div>

            <div className="success-message">
              <CheckCircle size={20} />
              <div>
                <strong>That's it!</strong> After your job runs, you'll see all these metrics appear in the dashboard.
                The platform automatically detects anomalies using statistical analysis and can send alerts if configured.
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Additional Resources */}
      <div className="resources-section">
        <h3>
          <BookOpen size={20} />
          Additional Resources
        </h3>
        <div className="resources-grid">
          <div className="resource-card">
            <h4>Example Scripts</h4>
            <p>Check the <code>scripts/</code> directory in the repository for ready-to-run demo scripts</p>
          </div>
          <div className="resource-card">
            <h4>Configuration Options</h4>
            <p>Customize listener behavior with additional Spark configurations for sampling, batching, and more</p>
          </div>
          <div className="resource-card">
            <h4>Alert Setup</h4>
            <p>Once data is flowing, configure email or Slack alerts to stay informed about anomalies</p>
          </div>
        </div>
      </div>

      {/* <div className="help-footer">
        <p>
          Need help? Check the project README or reach out to your platform administrator.
        </p>
      </div> */}
    </div>
  );
}

export default GettingStarted;
