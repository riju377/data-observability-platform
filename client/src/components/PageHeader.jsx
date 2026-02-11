import './PageHeader.css';

function PageHeader({ title, description, icon: Icon, children }) {
    return (
        <div className="page-header-modern">
            <div className="page-header-content">
                {Icon && (
                    <div className="page-header-icon">
                        <Icon size={28} strokeWidth={2} />
                    </div>
                )}
                <div className="page-header-text">
                    <h1 className="page-title">{title}</h1>
                    {description && <p className="page-description">{description}</p>}
                </div>
                {children && (
                    <div className="page-header-actions">
                        {children}
                    </div>
                )}
            </div>
            <div className="page-header-divider"></div>
        </div>
    );
}



export default PageHeader;
